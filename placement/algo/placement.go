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
	"encoding/json"
	"errors"
	"sort"

	"github.com/m3db/m3cluster/placement"
)

var (
	errShardsWithDifferentReplicas = errors.New("invalid placement, found shards with different replicas")
)

// placementSnapshot implements placement.Snapshot
type placementSnapshot struct {
	hostShards   []*hostShards
	rf           int
	uniqueShards []uint32
}

func newEmptyPlacement(hosts []placement.Host, ids []uint32) placementSnapshot {
	hostShards := make([]*hostShards, len(hosts), len(hosts))
	for i, ph := range hosts {
		hostShards[i] = newEmptyHostShardsFromHost(ph)
	}

	return placementSnapshot{hostShards: hostShards, uniqueShards: ids, rf: 0}
}

func newPlacementFromGenericSnapshot(p placement.Snapshot) placementSnapshot {
	hss := make([]*hostShards, p.HostsLen())
	for i, phs := range p.HostShards() {
		hss[i] = newHostShards(phs)
	}
	return placementSnapshot{hostShards: hss, rf: p.Replicas(), uniqueShards: p.Shards()}
}

func newPlacement(hss []*hostShards, shards []uint32, rf int) placementSnapshot {
	return placementSnapshot{hostShards: hss, rf: rf, uniqueShards: shards}
}

func (ps placementSnapshot) HostShards() []placement.HostShards {
	result := make([]placement.HostShards, len(ps.hostShards))
	for i, hs := range ps.hostShards {
		result[i] = hs
	}
	return result
}

func (ps placementSnapshot) HostsLen() int {
	return len(ps.hostShards)
}

func (ps placementSnapshot) Replicas() int {
	return ps.rf
}

func (ps placementSnapshot) ShardsLen() int {
	return len(ps.uniqueShards)
}

func (ps placementSnapshot) Shards() []uint32 {
	return ps.uniqueShards
}

func (ps placementSnapshot) HostShard(address string) placement.HostShards {
	for _, phs := range ps.HostShards() {
		if phs.Host().Address == address {
			return phs
		}
	}
	return nil
}

// NewPlacementFromJSON creates a Snapshot from JSON
func NewPlacementFromJSON(data []byte) (placement.Snapshot, error) {
	var ps placementSnapshot
	if err := json.Unmarshal(data, &ps); err != nil {
		return nil, err
	}
	return ps, nil
}

func (ps placementSnapshot) MarshalJSON() ([]byte, error) {
	return json.Marshal(placementSnapshotToJSON(ps))
}

func placementSnapshotToJSON(ps placementSnapshot) hostShardsJSONs {
	hsjs := make(hostShardsJSONs, ps.HostsLen())
	for i, hs := range ps.hostShards {
		hsjs[i] = hostShardsToJSON(*hs)
	}
	sort.Sort(hsjs)
	return hsjs
}

func hostShardsToJSON(hs hostShards) hostShardsJSON {
	shards := hs.Shards()
	uintShards := sortableUInt32(shards)
	sort.Sort(uintShards)
	return hostShardsJSON{Address: hs.Host().Address, Rack: hs.Host().Rack, Shards: shards}
}

type sortableUInt32 []uint32

func (su sortableUInt32) Len() int {
	return len(su)
}

func (su sortableUInt32) Less(i, j int) bool {
	return int(su[i]) < int(su[j])
}

func (su sortableUInt32) Swap(i, j int) {
	su[i], su[j] = su[j], su[i]
}

func (ps *placementSnapshot) UnmarshalJSON(data []byte) error {
	var hsj hostShardsJSONs
	var err error
	if err = json.Unmarshal(data, &hsj); err != nil {
		return err
	}
	if *ps, err = placementSnapshotFromJSON(hsj); err != nil {
		return err
	}
	return nil
}

func placementSnapshotFromJSON(hsjs hostShardsJSONs) (placementSnapshot, error) {
	hss := make([]*hostShards, len(hsjs))
	shardsReplicaMap := make(map[uint32]int)
	for i, hsj := range hsjs {
		hss[i] = hostShardsFromJSON(hsj)
		for shard := range hss[i].shardsSet {
			shardsReplicaMap[shard] = shardsReplicaMap[shard] + 1
		}
	}
	shards := make([]uint32, 0, len(shardsReplicaMap))
	snapshotReplica := -1
	for shard, r := range shardsReplicaMap {
		shards = append(shards, shard)
		if snapshotReplica < 0 {
			snapshotReplica = r
			continue
		}
		if snapshotReplica != r {
			return placementSnapshot{}, errShardsWithDifferentReplicas
		}
	}
	return newPlacement(hss, shards, snapshotReplica), nil
}

type hostShardsJSONs []hostShardsJSON

func (hsj hostShardsJSONs) Len() int {
	return len(hsj)
}

func (hsj hostShardsJSONs) Less(i, j int) bool {
	if hsj[i].Rack == hsj[j].Rack {
		return hsj[i].Address < hsj[j].Address
	}
	return hsj[i].Rack < hsj[j].Rack
}

func (hsj hostShardsJSONs) Swap(i, j int) {
	hsj[i], hsj[j] = hsj[j], hsj[i]
}

type hostShardsJSON struct {
	Address string
	Rack    string
	Shards  []uint32
}

func hostShardsFromJSON(hsj hostShardsJSON) *hostShards {
	hs := newEmptyHostShards(hsj.Address, hsj.Rack)
	for _, shard := range hsj.Shards {
		hs.shardsSet[shard] = struct{}{}
	}
	return hs
}

// hostShards implements placement.HostShards
type hostShards struct {
	host      placement.Host
	shardsSet map[uint32]struct{}
}

func newEmptyHostShardsFromHost(host placement.Host) *hostShards {
	m := make(map[uint32]struct{})
	return &hostShards{host: host, shardsSet: m}
}

func newHostShards(hs placement.HostShards) *hostShards {
	shards := hs.Shards()
	m := make(map[uint32]struct{}, len(shards))
	for _, s := range shards {
		m[s] = struct{}{}
	}
	return &hostShards{host: hs.Host(), shardsSet: m}
}

func newEmptyHostShards(address, rack string) *hostShards {
	return &hostShards{host: newHost(address, rack), shardsSet: make(map[uint32]struct{})}
}

func (h hostShards) Host() placement.Host {
	return h.host
}

func (h hostShards) Shards() []uint32 {
	s := make([]uint32, 0, len(h.shardsSet))
	for shard := range h.shardsSet {
		s = append(s, shard)
	}
	return s
}

func (h hostShards) addShard(s uint32) {
	h.shardsSet[s] = struct{}{}
}

func (h hostShards) removeShard(shard uint32) {
	delete(h.shardsSet, shard)
}

func (h hostShards) shardLen() int {
	return len(h.shardsSet)
}

func (h hostShards) hostAddress() string {
	return h.host.Address

}

func (h hostShards) hostRack() string {
	return h.host.Rack
}

func (h hostShards) isSharingShard(other hostShards) bool {
	for p := range other.shardsSet {
		if _, exist := h.shardsSet[p]; exist {
			return true
		}
	}
	return false
}

func newHost(address, rack string) placement.Host {
	return placement.Host{Address: address, Rack: rack}
}
