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

package service

import (
	"errors"

	"github.com/m3db/m3cluster/placement"
)

var (
	errInvalidShardLen = errors.New("shardLen should be greater than 0")
	errHostAbsent      = errors.New("could not remove or replace a host that does not exist")
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

func (ps placementService) BuildInitialPlacement(service string, addresses []string, shardLen int) error {
	if shardLen <= 0 {
		return errInvalidShardLen
	}

	var phs []placement.Host
	var err error
	if phs, err = ps.getHostsFromInventory(addresses); err != nil {
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

func (ps placementService) AddHost(service string, address string) error {
	var s placement.Snapshot
	var err error
	if s, err = ps.Snapshot(service); err != nil {
		return err
	}

	var ph placement.Host
	if ph, err = ps.getHostFromInventory(address); err != nil {
		return err
	}

	if s, err = ps.algo.AddHost(s, ph); err != nil {
		return err
	}
	return ps.ss.SaveSnapshotForService(service, s)
}

func (ps placementService) RemoveHost(service string, leavingHostName string) error {
	var s placement.Snapshot
	var err error
	if s, err = ps.Snapshot(service); err != nil {
		return err
	}

	leavingHost, err := getHostFromPlacement(s, leavingHostName)
	if err != nil {
		return err
	}

	if s, err = ps.algo.RemoveHost(s, leavingHost); err != nil {
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

	leavingHost, err := getHostFromPlacement(s, leavingHostName)
	if err != nil {
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

func (ps placementService) Snapshot(service string) (placement.Snapshot, error) {
	return ps.ss.ReadSnapshotForService(service)
}

func getHostFromPlacement(s placement.Snapshot, host string) (placement.Host, error) {
	hs := s.HostShard(host)
	if hs == nil {
		return nil, errHostAbsent
	}
	return hs.Host(), nil
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
		return nil, err
	}
	return placement.NewHost(host, rack), nil
}
