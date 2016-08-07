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
	"sort"

	"github.com/m3db/m3cluster/placement"
)

var (
	errInvalidShardLen = errors.New("shardLen should be greater than 0")
	errHostAbsent      = errors.New("could not remove or replace a host that does not exist")
	errNoValidHost     = errors.New("could not find valid host to be added")
	errDuplicatedHost  = errors.New("could not place shards on duplicated hosts")
)

type placementService struct {
	algo placement.Algorithm
	ss   placement.SnapshotStorage
	hi   placement.HostInventory
}

// NewPlacementService returns an instance of placement service
func NewPlacementService(algo placement.Algorithm, ss placement.SnapshotStorage, hi placement.HostInventory) placement.Service {
	return placementService{algo: algo, ss: ss, hi: hi}
}

func (ps placementService) BuildInitialPlacement(service string, hosts []string, shardLen int) error {
	if shardLen <= 0 {
		return errInvalidShardLen
	}

	var phs []placement.Host
	var err error
	if phs, err = ps.getHostsFromInventory(hosts); err != nil {
		return err
	}

	ids := make([]uint32, shardLen)
	for i := 0; i < shardLen; i++ {
		ids[i] = uint32(i)
	}

	var s placement.Snapshot
	if s, err = ps.algo.BuildInitialPlacement(phs, ids); err != nil {
		return err
	}
	return ps.ss.SaveSnapshotForService(service, s)
}

func (ps placementService) AddReplica(service string) error {
	var s placement.Snapshot
	var err error
	if s, err = ps.Snapshot(service); err != nil {
		return err
	}

	if s, err = ps.algo.AddReplica(s); err != nil {
		return err
	}
	return ps.ss.SaveSnapshotForService(service, s)
}

func (ps placementService) AddHost(service string, host string) error {
	var s placement.Snapshot
	var err error
	if s, err = ps.Snapshot(service); err != nil {
		return err
	}

	var ph placement.Host
	if ph, err = ps.getHostFromInventory(host); err != nil {
		return err
	}

	if s, err = ps.algo.AddHost(s, ph); err != nil {
		return err
	}
	return ps.ss.SaveSnapshotForService(service, s)
}

func (ps placementService) AddHostFromPool(service string, pool string) error {
	var s placement.Snapshot
	var err error
	if s, err = ps.Snapshot(service); err != nil {
		return err
	}

	var ph placement.Host
	if ph, err = ps.findBestHostFromPool(s, pool, nil); err != nil {
		return err
	}

	if s, err = ps.algo.AddHost(s, ph); err != nil {
		return err
	}
	return ps.ss.SaveSnapshotForService(service, s)
}

func (ps placementService) RemoveHost(service string, host string) error {
	var s placement.Snapshot
	var err error
	if s, err = ps.Snapshot(service); err != nil {
		return err
	}

	var ph placement.Host
	if ph, err = ps.getHostFromPlacement(s, host); err != nil {
		return err
	}

	if s, err = ps.algo.RemoveHost(s, ph); err != nil {
		return err
	}
	return ps.ss.SaveSnapshotForService(service, s)
}

func (ps placementService) ReplaceHost(service string, leavingHostName string, addingHostName string) error {
	var s placement.Snapshot
	var err error
	if s, err = ps.Snapshot(service); err != nil {
		return err
	}

	var leavingHost placement.Host
	if leavingHost, err = ps.getHostFromPlacement(s, leavingHostName); err != nil {
		return err
	}

	var addingHost placement.Host
	if addingHost, err = ps.getHostFromInventory(addingHostName); err != nil {
		return err
	}

	if s, err = ps.algo.ReplaceHost(s, leavingHost, addingHost); err != nil {
		return err
	}
	return ps.ss.SaveSnapshotForService(service, s)
}

func (ps placementService) ReplaceHostFromPool(service string, leavingHostName string, pool string) error {
	var s placement.Snapshot
	var err error
	if s, err = ps.Snapshot(service); err != nil {
		return err
	}

	var leavingHost placement.Host
	if leavingHost, err = ps.getHostFromPlacement(s, leavingHostName); err != nil {
		return err
	}

	leavingRack := leavingHost.Rack
	var addingHost placement.Host
	if addingHost, err = ps.findBestHostFromPool(s, pool, &leavingRack); err != nil {
		return err
	}

	if s, err = ps.algo.ReplaceHost(s, leavingHost, addingHost); err != nil {
		return err
	}
	return ps.ss.SaveSnapshotForService(service, s)
}

func (ps placementService) Snapshot(service string) (placement.Snapshot, error) {
	return ps.ss.ReadSnapshotForService(service)
}

func (ps placementService) getHostFromPlacement(p placement.Snapshot, host string) (placement.Host, error) {
	for _, ph := range p.HostShards() {
		if ph.Host().Address == host {
			return ph.Host(), nil
		}
	}
	return placement.Host{}, errHostAbsent
}

func (ps placementService) findBestHostFromPool(p placement.Snapshot, pool string, preferRack *string) (placement.Host, error) {
	placementRackHostMap := make(map[string]map[string]struct{})
	for _, phs := range p.HostShards() {
		if _, exist := placementRackHostMap[phs.Host().Rack]; !exist {
			placementRackHostMap[phs.Host().Rack] = make(map[string]struct{})
		}
		placementRackHostMap[phs.Host().Rack][phs.Host().Address] = struct{}{}
	}

	// build rackHostMap from pool
	poolRackHostMap := ps.getRackHostMapFromPool(pool)

	// if there is a host from the preferred rack can be added, return it.
	if preferRack != nil {
		if hs, exist := poolRackHostMap[*preferRack]; exist {
			for _, host := range hs {
				if ps.isNewHostToPlacement(*preferRack, host, placementRackHostMap) {
					return ps.getHostFromInventory(host)
				}
			}
		}
	}

	// otherwise if there is a rack not in the current placement, prefer that rack
	for r, hosts := range poolRackHostMap {
		if _, exist := placementRackHostMap[r]; !exist {
			return ps.getHostFromInventory(hosts[0])
		}
	}

	// otherwise sort the racks in the current placement by capacity and find a valid host from inventory
	rackLens := make(rackLens, 0, len(placementRackHostMap))
	for rack, hs := range placementRackHostMap {
		rackLens = append(rackLens, rackLen{rack: rack, len: len(hs)})
	}
	sort.Sort(rackLens)

	for _, rackLen := range rackLens {
		if hs, exist := poolRackHostMap[rackLen.rack]; exist {
			for _, host := range hs {
				if ps.isNewHostToPlacement(rackLen.rack, host, placementRackHostMap) {
					return ps.getHostFromInventory(host)
				}
			}
		}
	}
	// no host in the inventory can be added to the placement
	return placement.Host{}, errNoValidHost
}

type rackLen struct {
	rack string
	len  int
}

type rackLens []rackLen

func (rls rackLens) Len() int {
	return len(rls)
}

func (rls rackLens) Less(i, j int) bool {
	return rls[i].len < rls[j].len
}

func (rls rackLens) Swap(i, j int) {
	rls[i], rls[j] = rls[j], rls[i]
}

func (ps placementService) isNewHostToPlacement(rack, host string, rackHost map[string]map[string]struct{}) bool {
	if _, rackExist := rackHost[rack]; !rackExist {
		return true
	}
	_, existHost := rackHost[rack][host]
	return !existHost
}

func (ps placementService) getHostsFromInventory(hosts []string) ([]placement.Host, error) {
	phs := make([]placement.Host, len(hosts))
	uniqueHost := make(map[string]struct{}, len(hosts))
	var err error
	for i, host := range hosts {
		if _, exist := uniqueHost[host]; exist {
			return nil, errDuplicatedHost
		}
		if phs[i], err = ps.getHostFromInventory(host); err != nil {
			return nil, err
		}
		uniqueHost[host] = struct{}{}
	}
	return phs, nil
}

func (ps placementService) getHostFromInventory(host string) (placement.Host, error) {
	rack, err := ps.hi.RackForHost(host)
	if err != nil {
		return placement.Host{}, err
	}
	return placement.Host{Address: host, Rack: rack}, nil
}

func (ps placementService) getRackHostMapFromPool(pool string) map[string][]string {
	hostsInPool := ps.hi.Dump(pool)
	result := make(map[string][]string, len(hostsInPool))
	for _, ph := range hostsInPool {
		if _, exist := result[ph.Rack]; !exist {
			result[ph.Rack] = make([]string, 0)
		}
		result[ph.Rack] = append(result[ph.Rack], ph.Address)
	}
	return result
}
