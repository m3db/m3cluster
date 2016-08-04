package algo

import (
	"github.com/m3db/m3cluster/placement"
)

// placementSnapshot implements placement.Snapshot
type placementSnapshot struct {
	hostShards   []*hostShards
	shardsLen    int
	rf           int
	uniqueShards []int
}

func newEmptyPlacement(hosts []placement.Host, ids []int) placementSnapshot {
	hostShards := make([]*hostShards, len(hosts), len(hosts))
	for i, ph := range hosts {
		hostShards[i] = newEmptyHostShardsFromHost(ph)
	}

	return placementSnapshot{hostShards: hostShards, shardsLen: len(ids), uniqueShards: ids, rf: 0}
}

func newM3DBPlacementFromPlacement(p placement.Snapshot) placementSnapshot {
	hss := make([]*hostShards, p.HostsLen())
	for i, phs := range p.HostShards() {
		hss[i] = newHostShards(phs)
	}
	return placementSnapshot{hostShards: hss, shardsLen: p.ShardsLen(), rf: p.Replicas(), uniqueShards: p.Shards()}
}

func newM3DBPlacement(hss []*hostShards, shards []int, rf int) placementSnapshot {
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

func (ps placementSnapshot) Shards() []int {
	return ps.uniqueShards
}

// hostShards implements placement.HostShards
type hostShards struct {
	h         host
	shardsSet map[int]struct{}
}

func newEmptyHostShardsFromHost(hs placement.Host) *hostShards {
	host := host{address: hs.Address(), rack: hs.Rack()}
	m := make(map[int]struct{})
	return &hostShards{h: host, shardsSet: m}
}

func newHostShards(hs placement.HostShards) *hostShards {
	host := host{address: hs.Host().Address(), rack: hs.Host().Rack()}
	shards := hs.Shards()
	m := make(map[int]struct{}, len(shards))
	for _, s := range shards {
		m[s] = struct{}{}
	}
	return &hostShards{h: host, shardsSet: m}
}

func newEmptyHostShards(address, rack string) *hostShards {
	return &hostShards{h: newHost(address, rack), shardsSet: make(map[int]struct{})}
}

func (h hostShards) Host() placement.Host {
	return h.h
}

func (h hostShards) Shards() []int {
	s := make([]int, 0, len(h.shardsSet))
	for shard := range h.shardsSet {
		s = append(s, shard)
	}
	return s
}

func (h hostShards) addShard(s int) {
	h.shardsSet[s] = struct{}{}
}

func (h hostShards) removeShard(shard int) {
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
