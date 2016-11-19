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

package placement

import (
	"errors"
	"fmt"
	"strings"

	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/shard"
)

var (
	errInvalidInstance     = errors.New("invalid shards assigned to an instance")
	errDuplicatedShards    = errors.New("invalid placement, there are duplicated shards in one replica")
	errUnexpectedShards    = errors.New("invalid placement, there are unexpected shard ids on instance")
	errTotalShardsMismatch = errors.New("invalid placement, the total shards in the placement does not match expected number")
	errInvalidShardsCount  = errors.New("invalid placement, the count for a shard does not match replica factor")
)

// NewPlacement returns a ServicePlacement
func NewPlacement(instances []services.PlacementInstance, shards []uint32, rf int) services.ServicePlacement {
	return placement{instances: instances, rf: rf, shards: shards}
}

type placement struct {
	instances []services.PlacementInstance
	rf        int
	shards    []uint32
}

func (p placement) Instances() []services.PlacementInstance {
	result := make([]services.PlacementInstance, p.NumInstances())
	for i, instance := range p.instances {
		result[i] = instance
	}
	return result
}

func (p placement) NumInstances() int {
	return len(p.instances)
}

func (p placement) Instance(id string) services.PlacementInstance {
	for _, instance := range p.Instances() {
		if instance.ID() == id {
			return instance
		}
	}
	return nil
}

func (p placement) ReplicaFactor() int {
	return p.rf
}

func (p placement) Shards() []uint32 {
	return p.shards
}

func (p placement) NumShards() int {
	return len(p.shards)
}

func (p placement) Validate() error {
	shardCountMap := ConvertShardSliceToMap(p.shards)
	if len(shardCountMap) != len(p.shards) {
		return errDuplicatedShards
	}

	expectedTotal := len(p.shards) * p.rf
	actualTotal := 0
	for _, instance := range p.instances {
		for _, id := range instance.Shards().ShardIDs() {
			if count, exist := shardCountMap[id]; exist {
				shardCountMap[id] = count + 1
				continue
			}

			return errUnexpectedShards
		}
		actualTotal += instance.Shards().NumShards()
	}

	if expectedTotal != actualTotal {
		return errTotalShardsMismatch
	}

	for shard, c := range shardCountMap {
		if p.rf != c {
			return fmt.Errorf("invalid shard count for shard %d: expected %d, actual %d", shard, p.rf, c)
		}
	}
	return nil
}

func (p placement) Copy() services.ServicePlacement {
	return placement{instances: copyInstances(p.Instances()), rf: p.ReplicaFactor(), shards: p.Shards()}
}

func copyInstances(instances []services.PlacementInstance) []services.PlacementInstance {
	copied := make([]services.PlacementInstance, len(instances))
	for i, instance := range instances {
		copied[i] = NewInstance().
			SetID(instance.ID()).
			SetName(instance.Name()).
			SetPort(instance.Port()).
			SetRack(instance.Rack()).
			SetZone(instance.Zone()).
			SetWeight(instance.Weight()).
			SetShards(shard.NewShardsWithIDs(instance.Shards().ShardIDs()))
	}
	return copied
}

// NewInstance returns a new PlacementInstance
func NewInstance() services.PlacementInstance {
	return &instance{shards: shard.NewShards(nil)}
}

// NewEmptyInstance returns a PlacementInstance with some basic properties but no shards assigned
func NewEmptyInstance(id, rack, zone string, weight uint32) services.PlacementInstance {
	return &instance{
		id:     id,
		rack:   rack,
		zone:   zone,
		weight: weight,
		shards: shard.NewShards(nil),
	}
}

type instance struct {
	id     string
	rack   string
	zone   string
	weight uint32
	name   string
	port   string
	shards shard.Shards
}

func (h *instance) String() string {
	return fmt.Sprintf("[id:%s, rack:%s, zone:%s, weight:%v]", h.id, h.rack, h.zone, h.weight)
}

func (h *instance) ID() string {
	return h.id
}

func (h *instance) SetID(id string) services.PlacementInstance {
	h.id = id
	return h
}

func (h *instance) Rack() string {
	return h.rack
}

func (h *instance) SetRack(r string) services.PlacementInstance {
	h.rack = r
	return h
}

func (h *instance) Zone() string {
	return h.zone
}

func (h *instance) SetZone(z string) services.PlacementInstance {
	h.zone = z
	return h
}

func (h *instance) Weight() uint32 {
	return h.weight
}

func (h *instance) SetWeight(w uint32) services.PlacementInstance {
	h.weight = w
	return h
}

func (h *instance) Name() string {
	return h.name
}

func (h *instance) SetName(n string) services.PlacementInstance {
	h.name = n
	return h
}

func (h *instance) Port() string {
	return h.port
}

func (h *instance) SetPort(p string) services.PlacementInstance {
	h.port = p
	return h
}

func (h *instance) Shards() shard.Shards {
	return h.shards
}

func (h *instance) SetShards(s shard.Shards) services.PlacementInstance {
	h.shards = s
	return h
}

// ByIDAscending sorts PlacementInstance by ID ascending
type ByIDAscending []services.PlacementInstance

func (s ByIDAscending) Len() int {
	return len(s)
}

func (s ByIDAscending) Less(i, j int) bool {
	return strings.Compare(s[i].ID(), s[j].ID()) < 0
}

func (s ByIDAscending) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// ConvertShardSliceToMap is an util function that converts a slice of shards to a map
func ConvertShardSliceToMap(ids []uint32) map[uint32]int {
	shardCounts := make(map[uint32]int)
	for _, id := range ids {
		shardCounts[id] = 0
	}
	return shardCounts
}
