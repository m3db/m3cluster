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
	"encoding/json"
	"errors"
	"sort"
)

var (
	errShardsWithDifferentReplicas = errors.New("invalid placement, found shards with different replicas")
)

// snapshot implements Snapshot
type snapshot struct {
	hostShards   []HostShards
	rf           int
	uniqueShards []uint32
}

// NewEmptyPlacementSnapshot returns an empty placement
func NewEmptyPlacementSnapshot(hosts []Host, ids []uint32) Snapshot {
	hostShards := make([]HostShards, len(hosts), len(hosts))
	for i, ph := range hosts {
		hostShards[i] = NewEmptyHostShardsFromHost(ph)
	}

	return snapshot{hostShards: hostShards, uniqueShards: ids, rf: 0}
}

// NewPlacementSnapshot returns a placement
func NewPlacementSnapshot(hss []HostShards, shards []uint32, rf int) Snapshot {
	return snapshot{hostShards: hss, rf: rf, uniqueShards: shards}
}

func (ps snapshot) HostShards() []HostShards {
	result := make([]HostShards, ps.HostsLen())
	for i, hs := range ps.hostShards {
		result[i] = hs
	}
	return result
}

func (ps snapshot) HostsLen() int {
	return len(ps.hostShards)
}

func (ps snapshot) Replicas() int {
	return ps.rf
}

func (ps snapshot) ShardsLen() int {
	return len(ps.uniqueShards)
}

func (ps snapshot) Shards() []uint32 {
	return ps.uniqueShards
}

func (ps snapshot) HostShard(address string) HostShards {
	for _, phs := range ps.HostShards() {
		if phs.Host().Address() == address {
			return phs
		}
	}
	return nil
}

// NewPlacementFromJSON creates a Snapshot from JSON
func NewPlacementFromJSON(data []byte) (Snapshot, error) {
	var ps snapshot
	if err := json.Unmarshal(data, &ps); err != nil {
		return nil, err
	}
	return ps, nil
}

func (ps snapshot) MarshalJSON() ([]byte, error) {
	return json.Marshal(placementSnapshotToJSON(ps))
}

func placementSnapshotToJSON(ps snapshot) hostShardsJSONs {
	hsjs := make(hostShardsJSONs, ps.HostsLen())
	for i, hs := range ps.hostShards {
		hsjs[i] = hostShardsToJSON(hs)
	}
	sort.Sort(hsjs)
	return hsjs
}

func hostShardsToJSON(hs HostShards) hostShardsJSON {
	shards := hs.Shards()
	uintShards := sortableUInt32(shards)
	sort.Sort(uintShards)
	return hostShardsJSON{Address: hs.Host().Address(), Rack: hs.Host().Rack(), Shards: shards}
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

func (ps *snapshot) UnmarshalJSON(data []byte) error {
	var hsj hostShardsJSONs
	var err error
	if err = json.Unmarshal(data, &hsj); err != nil {
		return err
	}
	if *ps, err = convertJSONtoSnapshot(hsj); err != nil {
		return err
	}
	return nil
}

func convertJSONtoSnapshot(hsjs hostShardsJSONs) (snapshot, error) {
	hss := make([]HostShards, len(hsjs))
	shardsReplicaMap := make(map[uint32]int)
	for i, hsj := range hsjs {
		hss[i] = hostShardsFromJSON(hsj)
		for _, shard := range hss[i].Shards() {
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
			return snapshot{}, errShardsWithDifferentReplicas
		}
	}
	return snapshot{hostShards: hss, uniqueShards: shards, rf: snapshotReplica}, nil
	//return NewPlacement(hss, shards, snapshotReplica), nil
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

func hostShardsFromJSON(hsj hostShardsJSON) HostShards {
	hs := NewEmptyHostShards(hsj.Address, hsj.Rack)
	for _, shard := range hsj.Shards {
		hs.AddShard(shard)
	}
	return hs
}

// hostShards implements HostShards
type hostShards struct {
	host      Host
	shardsSet map[uint32]struct{}
}

// NewEmptyHostShardsFromHost returns a HostShards with no shards assigned
func NewEmptyHostShardsFromHost(host Host) HostShards {
	m := make(map[uint32]struct{})
	return &hostShards{host: host, shardsSet: m}
}

// NewEmptyHostShards returns a HostShards with no shards assigned
func NewEmptyHostShards(address, rack string) HostShards {
	return NewEmptyHostShardsFromHost(NewHost(address, rack))
}

func (h hostShards) Host() Host {
	return h.host
}

func (h hostShards) Shards() []uint32 {
	s := make([]uint32, 0, len(h.shardsSet))
	for shard := range h.shardsSet {
		s = append(s, shard)
	}
	return s
}

func (h hostShards) AddShard(s uint32) {
	h.shardsSet[s] = struct{}{}
}

func (h hostShards) RemoveShard(shard uint32) {
	delete(h.shardsSet, shard)
}

func (h hostShards) ContainsShard(shard uint32) bool {
	if _, exist := h.shardsSet[shard]; exist {
		return true
	}
	return false
}

func (h hostShards) ShardsLen() int {
	return len(h.shardsSet)
}

// NewHost returns a Host
func NewHost(address, rack string) Host {
	return host{address: address, rack: rack}
}

type host struct {
	rack    string
	address string
}

func (h host) Address() string {
	return h.address
}

func (h host) Rack() string {
	return h.rack
}
