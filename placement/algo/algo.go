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
	"math"

	"github.com/m3db/m3cluster/placement"
)

var (
	errNotEnoughRacks          = errors.New("not enough racks to take shards, please make sure RF <= number of racks")
	errHostAbsent              = errors.New("could not remove or replace a host that does not exist")
	errCouldNotReachTargetLoad = errors.New("new host could not reach target load")
)

type rackAwarePlacementAlgorithm struct {
}

func newRackAwarePlacementAlgorithm() placement.Algorithm {
	return rackAwarePlacementAlgorithm{}
}

func (a rackAwarePlacementAlgorithm) BuildInitialPlacement(hosts []placement.Host, shards []uint32) (placement.Snapshot, error) {
	ph := newInitPlacementHelper(hosts, shards)

	if err := ph.placeShards(ph.uniqueShards, nil); err != nil {
		return nil, err
	}
	return ph.generatePlacement(), nil
}

func (a rackAwarePlacementAlgorithm) AddReplica(ps placement.Snapshot) (placement.Snapshot, error) {
	ph := newReplicaPlacementHelper(ps, ps.Replicas()+1)
	if err := ph.placeShards(ph.uniqueShards, nil); err != nil {
		return nil, err
	}
	return ph.generatePlacement(), nil
}

func (a rackAwarePlacementAlgorithm) RemoveHost(ps placement.Snapshot, leavingHost placement.Host) (placement.Snapshot, error) {
	if a.isHostAbsent(ps, leavingHost) {
		return nil, errHostAbsent
	}
	ph, leavingHostShards := newRemoveHostPlacementHelper(ps, leavingHost)
	// place the shards from the leaving host to the rest of the cluster
	if err := ph.placeShards(leavingHostShards.Shards(), leavingHostShards); err != nil {
		return nil, err
	}
	return ph.generatePlacement(), nil
}

func (a rackAwarePlacementAlgorithm) AddHost(ps placement.Snapshot, addingHost placement.Host) (placement.Snapshot, error) {
	ph, addingHostShards := newAddHostPlacementHelper(ps, addingHost)
	targetLoad := ph.hostHeap.getTargetLoadForHost(addingHostShards.hostAddress())
	// try to steal shards from the most loaded hosts until the adding host reaches target load
outer:
	for len(addingHostShards.shardsSet) < targetLoad {
		if ph.hostHeap.Len() == 0 {
			return nil, errCouldNotReachTargetLoad
		}
		tryHost := heap.Pop(ph.hostHeap).(*hostShards)
		for shard := range tryHost.shardsSet {
			if ph.canAssignHost(shard, tryHost, addingHostShards) {
				ph.assignShardToHost(shard, addingHostShards)
				ph.removeShardFromHost(shard, tryHost)
				heap.Push(ph.hostHeap, tryHost)
				continue outer
			}
		}
	}

	return ph.generatePlacement(), nil
}

func (a rackAwarePlacementAlgorithm) ReplaceHost(ps placement.Snapshot, leavingHost, addingHost placement.Host) (placement.Snapshot, error) {
	if a.isHostAbsent(ps, leavingHost) {
		return nil, errHostAbsent
	}
	addingHostShards := newEmptyHostShardsFromHost(addingHost)
	ph, leavingHostShards := newRemoveHostPlacementHelper(ps, leavingHost)

	var shardsUnassigned []uint32
	// move shards from leaving host to adding host
	for _, shard := range leavingHostShards.Shards() {
		if ph.canAssignHost(shard, leavingHostShards, addingHostShards) {
			ph.removeShardFromHost(shard, leavingHostShards)
			addingHostShards.addShard(shard)
		} else {
			shardsUnassigned = append(shardsUnassigned, shard)
		}
	}
	// if there are shards that can not be moved to adding host
	// distribute them to the cluster
	if err := ph.placeShards(shardsUnassigned, leavingHostShards); err != nil {
		return nil, err
	}

	cl := ph.generatePlacement()
	// add the adding host to the cluster and bring its load up to target load
	cl.hostShards = append(cl.hostShards, addingHostShards)
	return a.AddHost(cl, addingHost)
}

func (a rackAwarePlacementAlgorithm) isHostAbsent(ps placement.Snapshot, h placement.Host) bool {
	for _, hs := range ps.HostShards() {
		if hs.Host().Address == h.Address {
			return false
		}
	}
	return true
}

// shardAwareDeploymentPlanner plans the deployment so that as many hosts can be deployed
// at the same time without making more than 1 replica of any shard unavailable
type shardAwareDeploymentPlanner struct {
}

func newShardAwareDeploymentPlanner() placement.DeploymentPlanner {
	return shardAwareDeploymentPlanner{}
}

func (dp shardAwareDeploymentPlanner) DeploymentSteps(ps placement.Snapshot) [][]placement.HostShards {
	ph := newReplicaPlacementHelper(ps, ps.Replicas())
	var steps [][]placement.HostShards
	for ph.hostHeap.Len() > 0 {
		h := heap.Pop(ph.hostHeap).(*hostShards)
		var parallel []placement.HostShards
		parallel = append(parallel, h)
		var tried []*hostShards
		for ph.hostHeap.Len() > 0 {
			tryHost := heap.Pop(ph.hostHeap).(*hostShards)
			if !h.isSharingShard(*tryHost) {
				parallel = append(parallel, tryHost)
			} else {
				tried = append(tried, tryHost)
			}
		}
		for _, host := range tried {
			heap.Push(ph.hostHeap, host)
		}
		steps = append(steps, parallel)
	}
	return steps
}

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
	return newPlaceShardingHelper(emptyPlacement, emptyPlacement.rf+1, true)
}

func newReplicaPlacementHelper(s placement.Snapshot, targetRF int) *placementHelper {
	ps := newPlacementFromGenericSnapshot(s)
	return newPlaceShardingHelper(ps, targetRF, true)
}

func newAddHostPlacementHelper(s placement.Snapshot, host placement.Host) (*placementHelper, *hostShards) {
	var hosts []*hostShards
	var addingHost *hostShards
	for _, phs := range s.HostShards() {
		h := newHostShards(phs)
		if phs.Host().Address == host.Address {
			addingHost = h
		}
		hosts = append(hosts, h)
	}

	if addingHost == nil {
		addingHost = newEmptyHostShardsFromHost(host)
		hosts = append(hosts, addingHost)
	}

	ps := newPlacement(hosts, s.Shards(), s.Replicas())
	return newPlaceShardingHelper(ps, s.Replicas(), false), addingHost
}

func newRemoveHostPlacementHelper(s placement.Snapshot, host placement.Host) (*placementHelper, *hostShards) {
	var leavingHost *hostShards
	var hosts []*hostShards
	for _, phs := range s.HostShards() {
		h := newHostShards(phs)
		if phs.Host().Address == host.Address {
			leavingHost = h
			continue
		}
		hosts = append(hosts, h)
	}
	ps := newPlacement(hosts, s.Shards(), s.Replicas())
	return newPlaceShardingHelper(ps, s.Replicas(), true), leavingHost
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

	// build targetLoad map for hostHeap
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
	totalHostNumber := len(ph.hostShards)
	rackSizeMap := make(map[string]int)
	for rack := range ph.rackToHostsMap {
		rackHostNumber := len(ph.rackToHostsMap[rack])
		if float64(rackHostNumber)/float64(totalHostNumber) >= 1.0/float64(ph.rf) {
			overSizedRack++
			overSizedHosts += rackHostNumber
		}
		rackSizeMap[rack] = rackHostNumber
	}

	targetLoad := make(map[string]int)
	for _, host := range ph.hostShards {
		rackSize := rackSizeMap[host.hostRack()]
		if float64(rackSize)/float64(totalHostNumber) >= 1.0/float64(ph.rf) {
			// if the host is on a over-sized rack, the target load is topped at shardLen / rackSize
			targetLoad[host.hostAddress()] = int(math.Ceil(float64(ph.getShardLen()) / float64(rackSize)))
		} else {
			// if the host is on a normal rack, get the target load with aware of other over-sized rack
			targetLoad[host.hostAddress()] = ph.getShardLen() * (ph.rf - overSizedRack) / (totalHostNumber - overSizedHosts)
		}
	}

	ph.hostHeap = newHostHeap(ph.hostShards, hostCapacityAscending, targetLoad, ph.rackToHostsMap)
}

func (ph placementHelper) getHostLen() int {
	return len(ph.hostShards)
}

func (ph placementHelper) getShardLen() int {
	return len(ph.uniqueShards)
}

func (ph placementHelper) getAvgLoad() int {
	totalLoad := ph.rf * ph.getShardLen()
	numberOfHosts := ph.getHostLen()
	return totalLoad / numberOfHosts
}

func (ph placementHelper) getShardSet(shards []uint32) map[uint32]struct{} {
	shardSet := make(map[uint32]struct{}, len(shards))
	for _, shard := range shards {
		shardSet[shard] = struct{}{}
	}
	return shardSet
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
	if to == nil {
		return false
	}
	if _, exist := to.shardsSet[shard]; exist {
		return false
	}
	return ph.canAssignRack(shard, from, to.hostRack())
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
	shardSet := ph.getShardSet(shards)
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
	var triedHosts []*hostShards
	var sameRack []*hostShards
outer:
	for shard := range shards {
		triedHosts = triedHosts[:0]
		for ph.hostHeap.Len() > 0 {
			tryHost := heap.Pop(ph.hostHeap).(*hostShards)
			// do not place to same rack for now
			if from != nil && tryHost.hostRack() == from.hostRack() {
				sameRack = append(sameRack, tryHost)
				continue
			}
			triedHosts = append(triedHosts, tryHost)
			if ph.hostHeap.getTargetLoadForHost(tryHost.hostAddress())-len(tryHost.shardsSet) <= 0 {
				// this is where "some" is, at this point the best host option in the cluster
				// from a different rack has reached its target load, time to break out of the loop
				break outer
			}
			if ph.canAssignHost(shard, from, tryHost) {
				ph.assignShardToHost(shard, tryHost)
				delete(shards, shard)
				for _, triedHost := range triedHosts {
					heap.Push(ph.hostHeap, triedHost)
				}
				break
			}
		}
	}
	for _, host := range sameRack {
		heap.Push(ph.hostHeap, host)
	}
	for _, triedHost := range triedHosts {
		heap.Push(ph.hostHeap, triedHost)
	}
}

func (ph placementHelper) generatePlacement() placementSnapshot {
	return placementSnapshot{uniqueShards: ph.uniqueShards, rf: ph.rf, shardsLen: ph.getShardLen(), hostShards: ph.hostShards}
}

// hostHeap provides an easy way to get best candidate host to assign/steal a shard
type hostHeap struct {
	hosts                 []*hostShards
	rackToHostsMap        map[string]map[*hostShards]struct{}
	targetLoad2           map[string]int
	hostCapacityAscending bool
}

func newHostHeap(hosts []*hostShards, hostCapacityAscending bool, targetLoad map[string]int, rackToHostMap map[string]map[*hostShards]struct{}) *hostHeap {
	hHeap := &hostHeap{hostCapacityAscending: hostCapacityAscending, hosts: hosts, targetLoad2: targetLoad, rackToHostsMap: rackToHostMap}
	heap.Init(hHeap)
	return hHeap
}

func (hh hostHeap) getTargetLoadForHost(hostAddress string) int {
	return hh.targetLoad2[hostAddress]
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
