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

func (ps placement) Instances() []services.PlacementInstance {
	result := make([]services.PlacementInstance, ps.NumInstances())
	for i, hs := range ps.instances {
		result[i] = hs
	}
	return result
}

func (ps placement) NumInstances() int {
	return len(ps.instances)
}

func (ps placement) Instance(id string) services.PlacementInstance {
	for _, phs := range ps.Instances() {
		if phs.ID() == id {
			return phs
		}
	}
	return nil
}

func (ps placement) ReplicaFactor() int {
	return ps.rf
}

func (ps placement) Shards() []uint32 {
	return ps.shards
}

func (ps placement) NumShards() int {
	return len(ps.shards)
}

func (ps placement) Validate() error {
	shardCountMap := ConvertShardSliceToMap(ps.shards)
	if len(shardCountMap) != len(ps.shards) {
		return errDuplicatedShards
	}

	expectedTotal := len(ps.shards) * ps.rf
	actualTotal := 0
	for _, hs := range ps.instances {
		for _, id := range hs.Shards().ShardIDs() {
			if count, exist := shardCountMap[id]; exist {
				shardCountMap[id] = count + 1
				continue
			}

			return errUnexpectedShards
		}
		actualTotal += hs.Shards().NumShards()
	}

	if expectedTotal != actualTotal {
		return errTotalShardsMismatch
	}

	for shard, c := range shardCountMap {
		if ps.rf != c {
			return fmt.Errorf("invalid shard count for shard %d: expected %d, actual %d", shard, ps.rf, c)
		}
	}
	return nil
}

func (ps placement) Copy() services.ServicePlacement {
	return placement{instances: copyInstances(ps.Instances()), rf: ps.ReplicaFactor(), shards: ps.Shards()}
}

func copyInstances(hss []services.PlacementInstance) []services.PlacementInstance {
	copied := make([]services.PlacementInstance, len(hss))
	for i, hs := range hss {
		copied[i] = NewInstance(hs.ID(), hs.Rack(), hs.Zone(), hs.Weight(), hs.Shards().ShardIDs())
	}
	return copied
}

type instance struct {
	id     string
	rack   string
	zone   string
	weight uint32
	shards shard.Shards
}

// NewEmptyInstance returns a PlacementInstance with no shards assigned
func NewEmptyInstance(id, rack, zone string, weight uint32) services.PlacementInstance {
	return NewInstance(id, rack, zone, weight, nil)
}

// NewInstance returns a PlacementInstance with shards
func NewInstance(id, rack, zone string, weight uint32, shards []uint32) services.PlacementInstance {
	return &instance{
		id:     id,
		rack:   rack,
		zone:   zone,
		weight: weight,
		shards: shard.NewShards(shards),
	}
}

func (h instance) String() string {
	return fmt.Sprintf("[id:%s, rack:%s, zone:%s, weight:%v]", h.id, h.rack, h.zone, h.weight)
}

func (h instance) ID() string {
	return h.id
}

func (h instance) Rack() string {
	return h.rack
}

func (h instance) Zone() string {
	return h.zone
}

func (h instance) Weight() uint32 {
	return h.weight
}

func (h instance) Shards() shard.Shards {
	return h.shards
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
