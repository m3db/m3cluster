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
	"github.com/m3db/m3cluster/placement"
)

// placementSnapshot implements placement.Snapshot
type placementSnapshot struct {
	hostShards   []*hostShards
	shardsLen    int
	rf           int
	uniqueShards []uint32
}

func newEmptyPlacement(hosts []placement.Host, ids []uint32) placementSnapshot {
	hostShards := make([]*hostShards, len(hosts), len(hosts))
	for i, ph := range hosts {
		hostShards[i] = newEmptyHostShardsFromHost(ph)
	}

	return placementSnapshot{hostShards: hostShards, shardsLen: len(ids), uniqueShards: ids, rf: 0}
}

func newPlacementFromGenericSnapshot(p placement.Snapshot) placementSnapshot {
	hss := make([]*hostShards, p.HostsLen())
	for i, phs := range p.HostShards() {
		hss[i] = newHostShards(phs)
	}
	return placementSnapshot{hostShards: hss, shardsLen: p.ShardsLen(), rf: p.Replicas(), uniqueShards: p.Shards()}
}

func newPlacement(hss []*hostShards, shards []uint32, rf int) placementSnapshot {
	return placementSnapshot{hostShards: hss, shardsLen: len(shards), rf: rf, uniqueShards: shards}
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
	return ps.shardsLen
}

func (ps placementSnapshot) Shards() []uint32 {
	return ps.uniqueShards
}

// hostShards implements placement.HostShards
type hostShards struct {
	h         host
	shardsSet map[uint32]struct{}
}

func newEmptyHostShardsFromHost(hs placement.Host) *hostShards {
	host := host{address: hs.Address(), rack: hs.Rack()}
	m := make(map[uint32]struct{})
	return &hostShards{h: host, shardsSet: m}
}

func newHostShards(hs placement.HostShards) *hostShards {
	host := host{address: hs.Host().Address(), rack: hs.Host().Rack()}
	shards := hs.Shards()
	m := make(map[uint32]struct{}, len(shards))
	for _, s := range shards {
		m[s] = struct{}{}
	}
	return &hostShards{h: host, shardsSet: m}
}

func newEmptyHostShards(address, rack string) *hostShards {
	return &hostShards{h: newHost(address, rack), shardsSet: make(map[uint32]struct{})}
}

func (h hostShards) Host() placement.Host {
	return h.h
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
	return h.h.Address()
}

func (h hostShards) hostRack() string {
	return h.h.Rack()
}

func (h hostShards) isSharingShard(other hostShards) bool {
	for p := range other.shardsSet {
		if _, exist := h.shardsSet[p]; exist {
			return true
		}
	}
	return false
}

// host implements placement.Host
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

func newHost(address, rack string) host {
	return host{address: address, rack: rack}
}
