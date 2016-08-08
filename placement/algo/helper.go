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
	"math"

	"github.com/m3db/m3cluster/placement"
)

type placementHelper struct {
	hostHeap       *hostHeap
	shardToHostMap map[uint32]map[*hostShards]struct{}
	rackToHostsMap map[string]map[*hostShards]struct{}
	rf             int
	uniqueShards   []uint32
	hostShards     []*hostShards
}

func newInitPlacementHelper(hosts []placement.Host, ids []uint32) *placementHelper {
	emptyPlacement := newEmptyPlacement(hosts, ids)
	return newPlaceShardingHelper(emptyPlacement, emptyPlacement.Replicas()+1, true)
}

func newReplicaPlacementHelper(s placement.Snapshot, targetRF int) *placementHelper {
	ps := newPlacementFromGenericSnapshot(s)
	return newPlaceShardingHelper(ps, targetRF, true)
}

func newAddHostShardsPlacementHelper(s placement.Snapshot, hs *hostShards) (*placementHelper, error) {
	var hss []*hostShards

	if s.HostShard(hs.Host().Address) != nil {
		return nil, errHostAlreadyExist
	}

	for _, phs := range s.HostShards() {
		h := newHostShards(phs)
		hss = append(hss, h)
	}

	hss = append(hss, hs)

	ps := newPlacement(hss, s.Shards(), s.Replicas())
	return newPlaceShardingHelper(ps, s.Replicas(), false), nil
}

func newRemoveHostPlacementHelper(s placement.Snapshot, leavingHost placement.Host) (*placementHelper, *hostShards, error) {
	if s.HostShard(leavingHost.Address) == nil {
		return nil, nil, errHostAbsent
	}
	var leavingHostShards *hostShards
	var hosts []*hostShards
	for _, phs := range s.HostShards() {
		h := newHostShards(phs)
		if phs.Host().Address == leavingHost.Address {
			leavingHostShards = h
			continue
		}
		hosts = append(hosts, h)
	}
	ps := newPlacement(hosts, s.Shards(), s.Replicas())
	return newPlaceShardingHelper(ps, s.Replicas(), true), leavingHostShards, nil
}

func newPlaceShardingHelper(ps placementSnapshot, targetRF int, hostCapacityAscending bool) *placementHelper {
	ph := &placementHelper{
		shardToHostMap: make(map[uint32]map[*hostShards]struct{}),
		rackToHostsMap: make(map[string]map[*hostShards]struct{}),
		rf:             targetRF,
		hostShards:     ps.hostShards,
		uniqueShards:   ps.Shards(),
	}

	// build rackToHost map
	ph.buildRackToHostMap()

	ph.buildHostHeap(hostCapacityAscending)
	return ph
}

func (ph *placementHelper) buildRackToHostMap() {
	for _, h := range ph.hostShards {
		if _, exist := ph.rackToHostsMap[h.hostRack()]; !exist {
			ph.rackToHostsMap[h.hostRack()] = make(map[*hostShards]struct{})
		}
		ph.rackToHostsMap[h.hostRack()][h] = struct{}{}
		for shard := range h.shardsSet {
			ph.assignShardToHost(shard, h)
		}
	}
}

func (ph *placementHelper) buildHostHeap(hostCapacityAscending bool) {
	overSizedRack := 0
	overSizedHosts := 0
	rackSizeMap := make(map[string]int)
	for rack := range ph.rackToHostsMap {
		rackHostNumber := len(ph.rackToHostsMap[rack])
		if float64(rackHostNumber)/float64(ph.getHostLen()) >= 1.0/float64(ph.rf) {
			overSizedRack++
			overSizedHosts += rackHostNumber
		}
		rackSizeMap[rack] = rackHostNumber
	}

	targetLoadMap := ph.buildTargetLoadMap(rackSizeMap, overSizedRack, overSizedHosts)

	ph.hostHeap = newHostHeap(ph.hostShards, hostCapacityAscending, targetLoadMap, ph.rackToHostsMap)
}

func (ph *placementHelper) buildTargetLoadMap(rackSizeMap map[string]int, overSizedRackLen, overSizedHostLen int) map[string]int {
	targetLoad := make(map[string]int)
	for _, host := range ph.hostShards {
		rackSize := rackSizeMap[host.hostRack()]
		if float64(rackSize)/float64(ph.getHostLen()) >= 1.0/float64(ph.rf) {
			// if the host is on a over-sized rack, the target load is topped at shardLen / rackSize
			targetLoad[host.hostAddress()] = int(math.Ceil(float64(ph.getShardLen()) / float64(rackSize)))
		} else {
			// if the host is on a normal rack, get the target load with aware of other over-sized rack
			targetLoad[host.hostAddress()] = ph.getShardLen() * (ph.rf - overSizedRackLen) / (ph.getHostLen() - overSizedHostLen)
		}
	}
	return targetLoad
}

func (ph placementHelper) getHostLen() int {
	return len(ph.hostShards)
}

func (ph placementHelper) getShardLen() int {
	return len(ph.uniqueShards)
}

func (ph placementHelper) canAssignRack(shard uint32, from *hostShards, toRack string) bool {
	if from != nil {
		if from.hostRack() == toRack {
			return true
		}
	}
	for host := range ph.shardToHostMap[shard] {
		if host.hostRack() == toRack {
			return false
		}
	}
	return true
}

func (ph placementHelper) canAssignHost(shard uint32, from, to *hostShards) bool {
	if _, exist := to.shardsSet[shard]; exist {
		return false
	}
	return ph.canAssignRack(shard, from, to.hostRack())
}

func (ph placementHelper) moveOneShard(from, to *hostShards) bool {
	for shard := range from.shardsSet {
		if ph.moveShard(shard, from, to) {
			return true
		}
	}
	return false
}

func (ph placementHelper) moveShard(shard uint32, from, to *hostShards) bool {
	if ph.canAssignHost(shard, from, to) {
		ph.assignShardToHost(shard, to)
		ph.removeShardFromHost(shard, from)
		return true
	}
	return false
}

func (ph placementHelper) assignShardToHost(shard uint32, to *hostShards) {
	to.addShard(shard)

	if _, exist := ph.shardToHostMap[shard]; !exist {
		ph.shardToHostMap[shard] = make(map[*hostShards]struct{})
	}
	ph.shardToHostMap[shard][to] = struct{}{}
}

func (ph placementHelper) removeShardFromHost(shard uint32, from *hostShards) {
	from.removeShard(shard)

	delete(ph.shardToHostMap[shard], from)
}

func (ph placementHelper) placeShards(shards []uint32, from *hostShards) error {
	shardSet := convertToShardSet(shards)
	var tried []*hostShards

	if from != nil {
		// prefer to distribute "some" of the load to other racks first
		// because the load from a leaving host can always get assigned to a host on the same rack
		ph.placeToRacksOtherThanOrigin(shardSet, from)
	}

	// if there are shards left to be assigned, distribute them evenly
	for shard := range shardSet {
		tried = tried[:0]
		for ph.hostHeap.Len() > 0 {
			tryHost := heap.Pop(ph.hostHeap).(*hostShards)
			tried = append(tried, tryHost)
			if ph.canAssignHost(shard, from, tryHost) {
				ph.assignShardToHost(shard, tryHost)
				for _, triedHost := range tried {
					heap.Push(ph.hostHeap, triedHost)
				}
				break
			}
		}
		if ph.hostHeap.Len() == 0 {
			// this should only happen when RF > number of racks
			return errNotEnoughRacks
		}
	}
	return nil
}

// placeToRacksOtherThanOrigin move shards from a host to the rest of the cluster
// the goal of this function is to assign "some" of the shards to the hosts in other racks
func (ph placementHelper) placeToRacksOtherThanOrigin(shards map[uint32]struct{}, from *hostShards) {
	var sameRack []*hostShards
	var triedHosts []*hostShards
	for ph.hostHeap.Len() > 0 {
		tryHost := heap.Pop(ph.hostHeap).(*hostShards)
		if from != nil && tryHost.hostRack() == from.hostRack() {
			// do not place to same rack for now
			sameRack = append(sameRack, tryHost)
		} else {
			triedHosts = append(triedHosts, tryHost)
		}
	}
	for _, h := range triedHosts {
		heap.Push(ph.hostHeap, h)
	}

	triedHosts = triedHosts[:0]

outer:
	for shard := range shards {
		for ph.hostHeap.Len() > 0 {
			tryHost := heap.Pop(ph.hostHeap).(*hostShards)
			triedHosts = append(triedHosts, tryHost)
			if ph.hostHeap.getTargetLoadForHost(tryHost.hostAddress())-len(tryHost.shardsSet) <= 0 {
				// this is where "some" is, at this point the best host option in the cluster
				// from a different rack has reached its target load, time to break out of the loop
				break outer
			}
			if ph.canAssignHost(shard, from, tryHost) {
				ph.assignShardToHost(shard, tryHost)
				delete(shards, shard)
				break
			}
		}

		for _, triedHost := range triedHosts {
			heap.Push(ph.hostHeap, triedHost)
		}
		triedHosts = triedHosts[:0]
	}

	for _, host := range sameRack {
		heap.Push(ph.hostHeap, host)
	}
	for _, triedHost := range triedHosts {
		heap.Push(ph.hostHeap, triedHost)
	}
}

func (ph placementHelper) generatePlacement() placementSnapshot {
	return placementSnapshot{uniqueShards: ph.uniqueShards, rf: ph.rf, hostShards: ph.hostShards}
}

// hostHeap provides an easy way to get best candidate host to assign/steal a shard
type hostHeap struct {
	hosts                 []*hostShards
	rackToHostsMap        map[string]map[*hostShards]struct{}
	targetLoad            map[string]int
	hostCapacityAscending bool
}

func newHostHeap(hosts []*hostShards, hostCapacityAscending bool, targetLoad map[string]int, rackToHostMap map[string]map[*hostShards]struct{}) *hostHeap {
	hHeap := &hostHeap{hostCapacityAscending: hostCapacityAscending, hosts: hosts, targetLoad: targetLoad, rackToHostsMap: rackToHostMap}
	heap.Init(hHeap)
	return hHeap
}

func (hh hostHeap) getTargetLoadForHost(hostAddress string) int {
	return hh.targetLoad[hostAddress]
}
func (hh hostHeap) Len() int {
	return len(hh.hosts)
}

func (hh hostHeap) Less(i, j int) bool {
	hostI := hh.hosts[i]
	hostJ := hh.hosts[j]
	leftLoadOnI := hh.getTargetLoadForHost(hostI.hostAddress()) - len(hostI.shardsSet)
	leftLoadOnJ := hh.getTargetLoadForHost(hostJ.hostAddress()) - len(hostJ.shardsSet)
	// if both host has tokens to be filled, prefer the one on a bigger rack
	// since it tends to be more picky in accepting shards
	if leftLoadOnI > 0 && leftLoadOnJ > 0 {
		if hostI.hostRack() != hostJ.hostRack() {
			return len(hh.rackToHostsMap[hostI.hostRack()]) > len(hh.rackToHostsMap[hostJ.hostRack()])
		}
	}
	// compare left capacity on both hosts
	if hh.hostCapacityAscending {
		return leftLoadOnI > leftLoadOnJ
	}
	return leftLoadOnI < leftLoadOnJ
}

func (hh hostHeap) Swap(i, j int) {
	hh.hosts[i], hh.hosts[j] = hh.hosts[j], hh.hosts[i]
}

func (hh *hostHeap) Push(h interface{}) {
	host := h.(*hostShards)
	hh.hosts = append(hh.hosts, host)
}

func (hh *hostHeap) Pop() interface{} {
	n := len(hh.hosts)
	host := hh.hosts[n-1]
	hh.hosts = hh.hosts[0 : n-1]
	return host
}

func convertToShardSet(shards []uint32) map[uint32]struct{} {
	shardSet := make(map[uint32]struct{}, len(shards))
	for _, shard := range shards {
		shardSet[shard] = struct{}{}
	}
	return shardSet
}
