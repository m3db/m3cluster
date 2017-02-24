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
	"container/heap"
	"errors"
	"fmt"

	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/placement"
	"github.com/m3db/m3cluster/shard"
)

var (
	errNotEnoughRacks                   = errors.New("not enough racks to take shards, please make sure RF is less than number of racks")
	errAddingInstanceAlreadyExist       = errors.New("the adding instance is already in the placement")
	errCouldNotReachTargetLoad          = errors.New("new instance could not reach target load")
	errShardedAlgoOnNotShardedPlacement = errors.New("could not apply sharded algo on non-sharded placement")
)

type rackAwarePlacementAlgorithm struct {
	opts services.PlacementOptions
}

func newShardedAlgorithm(opts services.PlacementOptions) placement.Algorithm {
	return rackAwarePlacementAlgorithm{opts: opts}
}

func (a rackAwarePlacementAlgorithm) InitialPlacement(instances []services.PlacementInstance, shards []uint32) (services.ServicePlacement, error) {
	ph := newInitHelper(cloneInstances(instances), shards, a.opts)

	if err := ph.PlaceShards(newShards(shards), nil); err != nil {
		return nil, err
	}
	return ph.GeneratePlacement(false), nil
}

func (a rackAwarePlacementAlgorithm) AddReplica(p services.ServicePlacement) (services.ServicePlacement, error) {
	if !p.IsSharded() {
		return nil, errShardedAlgoOnNotShardedPlacement
	}

	p = clonePlacement(p)
	ph := newAddReplicaHelper(p, a.opts)
	if err := ph.PlaceShards(newShards(p.Shards()), nil); err != nil {
		return nil, err
	}
	return ph.GeneratePlacement(false), nil
}

func (a rackAwarePlacementAlgorithm) RemoveInstance(p services.ServicePlacement, instanceID string) (services.ServicePlacement, error) {
	if !p.IsSharded() {
		return nil, errShardedAlgoOnNotShardedPlacement
	}

	p = clonePlacement(p)
	ph, leavingInstance, err := newRemoveInstanceHelper(p, instanceID, a.opts)
	if err != nil {
		return nil, err
	}
	// place the shards from the leaving instance to the rest of the cluster
	if err := ph.PlaceShards(leavingInstance.Shards().All(), leavingInstance); err != nil {
		return nil, err
	}

	result, _, err := addInstanceToPlacement(ph.GeneratePlacement(false), leavingInstance, false)
	return result, err
}

func (a rackAwarePlacementAlgorithm) AddInstance(
	p services.ServicePlacement,
	i services.PlacementInstance,
) (services.ServicePlacement, error) {
	if !p.IsSharded() {
		return nil, errShardedAlgoOnNotShardedPlacement
	}

	return a.addInstance(clonePlacement(p), cloneInstance(i))
}

func (a rackAwarePlacementAlgorithm) ReplaceInstance(
	p services.ServicePlacement,
	instanceID string,
	addingInstances []services.PlacementInstance,
) (services.ServicePlacement, error) {
	if !p.IsSharded() {
		return nil, errShardedAlgoOnNotShardedPlacement
	}

	p = clonePlacement(p)
	ph, leavingInstance, addingInstances, err := newReplaceInstanceHelper(p, instanceID, addingInstances, a.opts)
	if err != nil {
		return nil, err
	}

	// move shards from leaving instance to adding instance
	instanceHeap := ph.BuildInstanceHeap(addingInstances, true)
	for loadOnInstance(leavingInstance) > 0 {
		if instanceHeap.Len() == 0 {
			break
		}
		tryInstance := heap.Pop(instanceHeap).(services.PlacementInstance)
		if moved := ph.MoveOneShard(leavingInstance, tryInstance); moved {
			heap.Push(instanceHeap, tryInstance)
		}
	}

	if loadOnInstance(leavingInstance) == 0 {
		result, _, err := addInstanceToPlacement(ph.GeneratePlacement(false), leavingInstance, false)
		return result, err
	}

	if !a.opts.AllowPartialReplace() {
		return nil, fmt.Errorf("could not fully replace all shards from %s, %v shards left unassigned", leavingInstance.ID(), leavingInstance.Shards().NumShards())
	}

	// place the shards from the leaving instance to the rest of the cluster
	if err := ph.PlaceShards(
		leavingInstance.Shards().All(),
		leavingInstance,
	); err != nil {
		return nil, err
	}
	// fill up to the target load for added instances if have not already done so
	newPlacement := ph.GeneratePlacement(true)
	for _, addingInstance := range addingInstances {
		newPlacement, addingInstance, err = removeInstanceFromPlacement(newPlacement, addingInstance.ID())
		if err != nil {
			return nil, err
		}
		newPlacement, err = a.addInstance(newPlacement, addingInstance)
		if err != nil {
			return nil, err
		}
	}

	result, _, err := addInstanceToPlacement(newPlacement, leavingInstance, false)
	return result, err
}

func (a rackAwarePlacementAlgorithm) addInstance(p services.ServicePlacement, addingInstance services.PlacementInstance) (services.ServicePlacement, error) {
	if instance, exist := p.Instance(addingInstance.ID()); exist {
		if !placement.IsInstanceLeaving(instance) {
			return nil, errAddingInstanceAlreadyExist
		}
		p = a.moveLeavingShardsBack(p, instance)
		addingInstance = instance
	}

	ph := newAddInstanceHelper(p, addingInstance, a.opts)
	targetLoad := ph.TargetLoadForInstance(addingInstance.ID())
	// try to take shards from the most loaded instances until the adding instance reaches target load
	hh := ph.BuildInstanceHeap(nonLeavingInstances(p.Instances()), false)
	for addingInstance.Shards().NumShards() < targetLoad {
		if hh.Len() == 0 {
			return nil, errCouldNotReachTargetLoad
		}
		tryInstance := heap.Pop(hh).(services.PlacementInstance)
		if moved := ph.MoveOneShard(tryInstance, addingInstance); moved {
			heap.Push(hh, tryInstance)
		}
	}

	return ph.GeneratePlacement(false), nil
}

func (a rackAwarePlacementAlgorithm) moveLeavingShardsBack(p services.ServicePlacement, source services.PlacementInstance) services.ServicePlacement {
	ph := newAddInstanceHelper(p, source, a.opts)
	instanceID := source.ID()
	// since the instance does not know where did its shards go, we need to iterate the whole placement to find them
	for _, other := range p.Instances() {
		if other.ID() == instanceID {
			continue
		}
		for _, s := range other.Shards().ShardsForState(shard.Initializing) {
			if s.SourceID() == instanceID {
				// NB(cw) in very rare case, the leaving shards could not be taken back
				// but it's fine, the algo will fil up those load with other shards from the cluster
				ph.MoveShard(s, other, source)
			}
		}
	}

	return ph.GeneratePlacement(false)
}
