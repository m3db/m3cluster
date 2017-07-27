// Copyright (c) 2016 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package algo

import (
	"errors"
	"fmt"

	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/placement"
	"github.com/m3db/m3cluster/shard"
)

var (
	errMirrorAlgoOnNotMirroredPlacement = errors.New("could not apply mirrored algo on non-mirrored placement")
)

type mirroredAlgorithm struct {
	opts        services.PlacementOptions
	shardedAlgo placement.Algorithm
}

func newMirroredAlgorithm(opts services.PlacementOptions) placement.Algorithm {
	return mirroredAlgorithm{
		opts:        opts,
		shardedAlgo: newShardedAlgorithm(opts),
	}
}

func (a mirroredAlgorithm) InitialPlacement(
	instances []services.PlacementInstance,
	shards []uint32,
	rf int,
) (services.Placement, error) {
	mirrorInstances, err := groupInstancesByShardSetID(instances, rf)
	if err != nil {
		return nil, err
	}

	mirrorPlacement, err := a.shardedAlgo.InitialPlacement(mirrorInstances, shards, 1)
	if err != nil {
		return nil, err
	}

	return placementFromMirror(mirrorPlacement, instances, rf)
}

func (a mirroredAlgorithm) AddReplica(p services.Placement) (services.Placement, error) {
	// TODO(cw): We could support AddReplica(p services.Placement, instances []services.PlacementInstance)
	// and apply the shards from the new replica to the adding instances in the future.
	return nil, errors.New("not supported")
}

func (a mirroredAlgorithm) RemoveInstance(p services.Placement, instanceID string) (services.Placement, error) {
	return a.RemoveInstances(p, []string{instanceID})
}

func (a mirroredAlgorithm) RemoveInstances(
	p services.Placement,
	instanceIDs []string,
) (services.Placement, error) {
	if !p.IsMirrored() {
		return nil, errMirrorAlgoOnNotMirroredPlacement
	}

	removingInstances := make([]services.PlacementInstance, len(instanceIDs))
	for i, id := range instanceIDs {
		instance, ok := p.Instance(id)
		if !ok {
			return nil, fmt.Errorf("instance %s not found in placement", id)
		}
		removingInstances[i] = instance
	}

	mirrorPlacement, err := mirrorFromPlacement(p)
	if err != nil {
		return nil, err
	}
	mirrorInstances, err := groupInstancesByShardSetID(removingInstances, p.ReplicaFactor())
	if err != nil {
		return nil, err
	}

	for _, instance := range mirrorInstances {
		ph, leavingInstance, err := newRemoveInstanceHelper(mirrorPlacement, instance.ID(), a.opts)
		if err != nil {
			return nil, err
		}
		// place the shards from the leaving instance to the rest of the cluster
		if err := ph.PlaceShards(leavingInstance.Shards().All(), leavingInstance, ph.Instances()); err != nil {
			return nil, err
		}

		if mirrorPlacement, _, err = addInstanceToPlacement(ph.GeneratePlacement(nonEmptyOnly), leavingInstance, false); err != nil {
			return nil, err
		}
	}
	return placementFromMirror(mirrorPlacement, nonLeavingInstances(p.Instances()), p.ReplicaFactor())
}

func (a mirroredAlgorithm) AddInstance(
	p services.Placement,
	addingInstance services.PlacementInstance,
) (services.Placement, error) {
	return a.AddInstances(p, []services.PlacementInstance{addingInstance})
}

func (a mirroredAlgorithm) AddInstances(
	p services.Placement,
	addingInstances []services.PlacementInstance,
) (services.Placement, error) {
	if !p.IsMirrored() {
		return nil, errMirrorAlgoOnNotMirroredPlacement
	}

	for _, instance := range addingInstances {
		if _, exist := p.Instance(instance.ID()); exist {
			// TODO(cw): Should we allow adding a leaving instance?
			// What if leaving instance paired with new instances?
			return nil, fmt.Errorf("instance %s already exist in the placement", instance.ID())
		}
	}

	mirrorPlacement, err := mirrorFromPlacement(p)
	if err != nil {
		return nil, err
	}
	mirrorInstances, err := groupInstancesByShardSetID(addingInstances, p.ReplicaFactor())
	if err != nil {
		return nil, err
	}

	for _, instance := range mirrorInstances {
		if _, ok := mirrorPlacement.Instance(instance.ID()); ok {
			return nil, fmt.Errorf("shard set id %s already exist in current placement", instance.ShardSetID())
		}
		ph := newAddInstanceHelper(mirrorPlacement, instance, a.opts)
		ph.AddInstance(instance)
		mirrorPlacement = ph.GeneratePlacement(nonEmptyOnly)
	}

	return placementFromMirror(mirrorPlacement, nonLeavingInstances(append(p.Instances(), addingInstances...)), p.ReplicaFactor())
}

func (a mirroredAlgorithm) ReplaceInstance(
	p services.Placement,
	instanceID string,
	addingInstances []services.PlacementInstance,
) (services.Placement, error) {
	if !p.IsMirrored() {
		return nil, errMirrorAlgoOnNotMirroredPlacement
	}

	if len(addingInstances) != 1 {
		return nil, fmt.Errorf("invalid number of instances: %d for mirrored replace", len(addingInstances))
	}
	addingInstance := placement.CloneInstance(addingInstances[0])

	// Clean up shard states before the replacement operation.
	mirror, err := mirrorFromPlacement(p)
	if err != nil {
		return nil, err
	}

	p, err = placementFromMirror(mirror, nonLeavingInstances(p.Instances()), p.ReplicaFactor())
	if err != nil {
		return nil, err
	}

	leavingInstance, ok := p.Instance(instanceID)
	if !ok {
		return nil, fmt.Errorf("instance %s not found in placement", instanceID)
	}

	shards := leavingInstance.Shards()
	for _, s := range shards.All() {
		// NB(cw): Mark Available as Initializing in case we use shard state
		// to indicate cutover/cutoff time for the shard.
		if s.State() == shard.Available {
			s.SetState(shard.Initializing)
		}
	}

	addingInstance = addingInstance.SetShards(shards).SetShardSetID(leavingInstance.ShardSetID())

	// NB(cw): Replace is supposed to be used when the leaving node is considered dead,
	// so the leaving node will be removed from the placement in this algo, unlike the
	// replacement of non-mirrored algo which will leave the leaving node in the
	// placement with all shards marked as Leaving.
	// This is because we use shardSetID as the sourceID in mirrored placement for
	// initializing shards, and I don't want the adding instance to keep a sourceID
	// of the leaving instance for this case.
	instances := placement.RemoveInstanceFromList(p.Instances(), instanceID)
	instances = append(instances, addingInstance)
	return placement.NewPlacement().
		SetInstances(instances).
		SetReplicaFactor(p.ReplicaFactor()).
		SetShards(p.Shards()).
		SetIsSharded(p.IsSharded()).
		SetIsMirrored(p.IsMirrored()), nil
}

func groupInstancesByShardSetID(
	instances []services.PlacementInstance,
	rf int,
) ([]services.PlacementInstance, error) {
	var (
		shardSetMap   = make(map[string]uint32, len(instances))
		shardSetCount = make(map[string]int, len(instances))
		res           = make([]services.PlacementInstance, 0, len(instances))
	)
	for _, instance := range instances {
		ssID := instance.ShardSetID()
		weight := instance.Weight()
		shardSetCount[ssID] = shardSetCount[ssID] + 1
		if w, ok := shardSetMap[ssID]; ok {
			if weight == w {
				continue
			}
			return nil, fmt.Errorf("found different weights: %d and %d, for shardset id %s", w, weight, ssID)
		}

		shardSetMap[ssID] = instance.Weight()
		res = append(
			res,
			placement.NewInstance().
				SetID(ssID).
				SetRack(ssID).
				SetWeight(weight).
				SetShardSetID(instance.ShardSetID()).
				SetShards(instance.Shards()),
		)
	}

	for ssID, count := range shardSetCount {
		if count != rf {
			return nil, fmt.Errorf("got %d count of shard set id %s, expecting %d", count, ssID, rf)
		}
	}

	return res, nil
}

// mirrorFromPlacement zips all instances with the same shardSetID into a virtual instance
// and create a placement with those virtual instance and rf=1.
func mirrorFromPlacement(p services.Placement) (services.Placement, error) {
	mirrorInstances, err := groupInstancesByShardSetID(p.Instances(), p.ReplicaFactor())
	if err != nil {
		return nil, err
	}

	mirror := placement.NewPlacement().
		SetInstances(mirrorInstances).
		SetReplicaFactor(1).
		SetShards(p.Shards()).
		SetIsSharded(true).
		SetIsMirrored(true)
	// NB(cw): Always clean up the shard states from previous placement changes,
	// so the shard states in the new placement only reflects the shard ownership
	// change from the latest operation.
	return placement.MarkAllShardsAsAvailable(mirror)
}

// placementFromMirror duplicates the shards for each shard set id and assign
// them to the instance with the shard set id.
func placementFromMirror(
	mirror services.Placement,
	instances []services.PlacementInstance,
	rf int,
) (services.Placement, error) {
	var (
		mirrorInstances = mirror.Instances()
		shardSetMap     = make(map[string]shard.Shards, len(mirrorInstances))
	)
	for _, instance := range mirrorInstances {
		shardSetMap[instance.ShardSetID()] = instance.Shards()
	}
	for _, instance := range instances {
		shards, ok := shardSetMap[instance.ShardSetID()]
		if !ok {
			return nil, fmt.Errorf("could not find shardset id %s for instance %s", instance.ShardSetID(), instance.ID())
		}
		instance.SetShards(shards)
	}

	return placement.NewPlacement().
		SetInstances(instances).
		SetReplicaFactor(rf).
		SetShards(mirror.Shards()).
		SetIsMirrored(true).
		SetIsSharded(true), nil
}
