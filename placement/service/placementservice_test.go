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
	"io/ioutil"
	"os"
	"testing"

	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3cluster/placement/algo"
	"github.com/stretchr/testify/assert"
)

func TestGoodWorkflow(t *testing.T) {
	ps := NewPlacementService(algo.NewRackAwarePlacementAlgorithm(), NewMockStorage(), mockInventory{})

	err := ps.BuildInitialPlacement("serviceA", []string{"r1h1", "r2h2"}, 10)
	assert.NoError(t, err)

	err = ps.AddReplica("serviceA")
	assert.NoError(t, err)

	err = ps.AddHost("serviceA", "r3h3")
	assert.NoError(t, err)

	err = ps.AddHostFromPool("serviceA", "a pool")
	assert.NoError(t, err)

	err = ps.RemoveHost("serviceA", "r1h1")
	assert.NoError(t, err)

	err = ps.ReplaceHost("serviceA", "r2h2", "r2h3")
	assert.NoError(t, err)

	err = ps.ReplaceHostFromPool("serviceA", "r3h3", "a pool")
	assert.NoError(t, err)

	cleanUpTestFiles(t, "serviceA")
}

func TestBadInitialPlacement(t *testing.T) {
	ps := NewPlacementService(algo.NewRackAwarePlacementAlgorithm(), NewMockStorage(), mockInventory{})

	err := ps.BuildInitialPlacement("serviceA", []string{"r1h1", "r2h2"}, 0)
	assert.Error(t, err)

	// host not found in inventory
	err = ps.BuildInitialPlacement("serviceA", []string{"bad1", "bad2"}, 10)
	assert.Error(t, err)

	// not enough racks/hosts
	err = ps.BuildInitialPlacement("serviceA", []string{}, 10)
	assert.Error(t, err)
}

func TestBadAddReplica(t *testing.T) {
	ps := NewPlacementService(algo.NewRackAwarePlacementAlgorithm(), NewMockStorage(), mockInventory{})

	err := ps.BuildInitialPlacement("serviceA", []string{"r1h1"}, 10)
	assert.NoError(t, err)

	// not enough racks/hosts
	err = ps.AddReplica("serviceA")
	assert.Error(t, err)

	// could not find snapshot for service
	err = ps.AddReplica("badService")
	assert.Error(t, err)

	cleanUpTestFiles(t, "serviceA")
}

func TestBadAddHost(t *testing.T) {
	ps := NewPlacementService(algo.NewRackAwarePlacementAlgorithm(), NewMockStorage(), mockInventory{})

	err := ps.BuildInitialPlacement("serviceA", []string{"r1h1"}, 10)
	assert.NoError(t, err)

	psWithEmptyInventory := NewPlacementService(algo.NewRackAwarePlacementAlgorithm(), NewMockStorage(), emptyInventory{})
	// could not find host in inventory
	err = psWithEmptyInventory.AddHost("serviceA", "r4h4")
	assert.Error(t, err)

	// algo error
	psWithErrorAlgo := NewPlacementService(errorAlgorithm{}, NewMockStorage(), mockInventory{})
	psWithErrorAlgo.AddHost("serviceA", "r2h2")
	assert.Error(t, err)

	// could not find snapshot for service
	err = ps.AddHost("badService", "r2h2")
	assert.Error(t, err)

	cleanUpTestFiles(t, "serviceA")
}

func TestBadAddHostFromPool(t *testing.T) {
	ps := NewPlacementService(algo.NewRackAwarePlacementAlgorithm(), NewMockStorage(), mockInventory{})

	err := ps.BuildInitialPlacement("serviceA", []string{"r1h1"}, 10)
	assert.NoError(t, err)

	// could not find host in inventory
	psWithEmptyInventory := NewPlacementService(algo.NewRackAwarePlacementAlgorithm(), NewMockStorage(), emptyInventory{})
	err = psWithEmptyInventory.AddHostFromPool("serviceA", "poolNotExist")
	assert.Error(t, err)

	// algo error
	psWithErrorAlgo := NewPlacementService(errorAlgorithm{}, NewMockStorage(), mockInventory{})
	psWithErrorAlgo.AddHostFromPool("serviceA", "pool")
	assert.Error(t, err)

	// could not find snapshot for service
	err = ps.AddHostFromPool("badService", "r2h2")
	assert.Error(t, err)

	cleanUpTestFiles(t, "serviceA")
}

func TestBadRemoveHost(t *testing.T) {
	ps := NewPlacementService(algo.NewRackAwarePlacementAlgorithm(), NewMockStorage(), mockInventory{})

	err := ps.BuildInitialPlacement("serviceA", []string{"r1h1"}, 10)
	assert.NoError(t, err)

	// not enough racks/hosts after removal
	err = ps.RemoveHost("serviceA", "r1h1")
	assert.Error(t, err)

	// host not exist
	err = ps.RemoveHost("serviceA", "hostNotExist")
	assert.Error(t, err)

	// could not find snapshot for service
	err = ps.RemoveHost("bad service", "r1h1")
	assert.Error(t, err)

	cleanUpTestFiles(t, "serviceA")
}

func TestBadReplaceHost(t *testing.T) {
	ps := NewPlacementService(algo.NewRackAwarePlacementAlgorithm(), NewMockStorage(), mockInventory{})

	err := ps.BuildInitialPlacement("serviceA", []string{"r1h1", "r4h4"}, 10)
	assert.NoError(t, err)

	// leaving host not exist
	err = ps.ReplaceHost("serviceA", "r1h2", "r2h2")
	assert.Error(t, err)

	// adding host not exist
	psWithEmptyInventory := NewPlacementService(algo.NewRackAwarePlacementAlgorithm(), NewMockStorage(), emptyInventory{})
	err = psWithEmptyInventory.ReplaceHost("serviceA", "r1h1", "r2h2")
	assert.Error(t, err)

	// not enough rack after replace
	err = ps.AddReplica("serviceA")
	assert.NoError(t, err)
	err = ps.ReplaceHost("serviceA", "r4h4", "r1h2")
	assert.Error(t, err)

	// could not find snapshot for service
	err = ps.ReplaceHost("badService", "r1h1", "r2h2")
	assert.Error(t, err)

	cleanUpTestFiles(t, "serviceA")
}

func TestBadReplaceHostFromPool(t *testing.T) {
	ps := NewPlacementService(algo.NewRackAwarePlacementAlgorithm(), NewMockStorage(), mockInventory{})

	err := ps.BuildInitialPlacement("serviceA", []string{"r1h1", "r4h4"}, 10)
	assert.NoError(t, err)

	// leaving host not exist
	err = ps.ReplaceHostFromPool("serviceA", "r1h2", "pool")
	assert.Error(t, err)

	// could not find host in inventory
	psWithEmptyInventory := NewPlacementService(algo.NewRackAwarePlacementAlgorithm(), NewMockStorage(), emptyInventory{})
	err = psWithEmptyInventory.ReplaceHostFromPool("serviceA", "r1h1", "emptyPool")
	assert.Error(t, err)

	// not enough rack after replace
	err = ps.AddReplica("serviceA")
	assert.NoError(t, err)
	psWithSmallInventory := NewPlacementService(algo.NewRackAwarePlacementAlgorithm(), NewMockStorage(), inventoryWithRackOneOnly{})
	err = psWithSmallInventory.ReplaceHostFromPool("serviceA", "r4h4", "pool")
	assert.Error(t, err)

	// could not find snapshot for service
	err = ps.ReplaceHostFromPool("badService", "r1h1", "pool")
	assert.Error(t, err)

	cleanUpTestFiles(t, "serviceA")
}

func TestGetHostFromPool(t *testing.T) {
	ps := NewPlacementService(algo.NewRackAwarePlacementAlgorithm(), NewMockStorage(), mockInventory{}).(placementService)
	// duplicated host
	hosts, err := ps.getHostsFromInventory([]string{"r1h1", "r1h1"})
	assert.Nil(t, hosts)
	assert.Error(t, err)

	// host not exist
	hosts, err = ps.getHostsFromInventory([]string{"badHost", "r1h1"})
	assert.Nil(t, hosts)
	assert.Error(t, err)

	hosts, err = ps.getHostsFromInventory([]string{"r1h1", "r2h2"})
	assert.Equal(t, 2, len(hosts))
	assert.NoError(t, err)
}

func TestIsNewHostToPlacement(t *testing.T) {
	ps := NewPlacementService(algo.NewRackAwarePlacementAlgorithm(), NewMockStorage(), mockInventory{}).(placementService)

	currentPlacement := map[string]map[string]struct{}{
		"r1": map[string]struct{}{
			"h1": struct{}{},
		},
	}
	// new rack
	isNew := ps.isNewHostToPlacement("rackNew", "h100", currentPlacement)
	assert.True(t, isNew)

	// new host
	isNew = ps.isNewHostToPlacement("r1", "h100", currentPlacement)
	assert.True(t, isNew)

	// old host
	isNew = ps.isNewHostToPlacement("r1", "h1", currentPlacement)
	assert.False(t, isNew)
}

func TestFindBestHostFromPool(t *testing.T) {
	ps := NewPlacementService(algo.NewRackAwarePlacementAlgorithm(), NewMockStorage(), mockInventory{}).(placementService)
	err := ps.BuildInitialPlacement("serviceA", []string{"r1h1", "r2h1", "r2h2", "r3h1", "r3h2", "r3h3"}, 100)
	assert.NoError(t, err)
	s, err := ps.Snapshot("serviceA")
	assert.NoError(t, err)

	// find host with preferred rack
	preferredRack := "r1"
	host, err := ps.findBestHostFromPool(s, "pool", &preferredRack)
	assert.NoError(t, err)
	assert.Equal(t, "r1", host.Rack)

	// find host in new rack
	host, err = ps.findBestHostFromPool(s, "pool", nil)
	assert.NoError(t, err)
	assert.Equal(t, "r4", host.Rack)

	// find host in least sized rack
	err = ps.AddHost("serviceA", "r4h1")
	err = ps.AddHost("serviceA", "r4h2")
	assert.NoError(t, err)
	s, err = ps.Snapshot("serviceA")
	host, err = ps.findBestHostFromPool(s, "pool", nil)
	assert.NoError(t, err)
	assert.Equal(t, "r1", host.Rack)

	// could not find valid host in inventory
	psWithEmptyInventory := NewPlacementService(algo.NewRackAwarePlacementAlgorithm(), NewMockStorage(), emptyInventory{}).(placementService)
	host, err = psWithEmptyInventory.findBestHostFromPool(s, "pool", nil)
	assert.Error(t, err)
	assert.Equal(t, placement.Host{}, host)
	cleanUpTestFiles(t, "serviceA")
}

func cleanUpTestFiles(t *testing.T, service string) {
	err := os.Remove(getSnapshotFileName(service))
	if err != nil {
		assert.FailNow(t, err.Error())
	}
}

type errorAlgorithm struct{}

func (errorAlgorithm) BuildInitialPlacement(hosts []placement.Host, ids []uint32) (placement.Snapshot, error) {
	return nil, errors.New("error in errorAlgorithm")
}

func (errorAlgorithm) AddReplica(p placement.Snapshot) (placement.Snapshot, error) {
	return nil, errors.New("error in errorAlgorithm")
}

func (errorAlgorithm) AddHost(p placement.Snapshot, h placement.Host) (placement.Snapshot, error) {
	return nil, errors.New("error in errorAlgorithm")
}

func (errorAlgorithm) RemoveHost(p placement.Snapshot, h placement.Host) (placement.Snapshot, error) {
	return nil, errors.New("error in errorAlgorithm")
}

func (errorAlgorithm) ReplaceHost(p placement.Snapshot, leavingHost, addingHost placement.Host) (placement.Snapshot, error) {
	return nil, errors.New("error in errorAlgorithm")
}

type mockInventory struct {
}

var (
	mockInventoryMap = map[string][]string{
		"r1": []string{"r1h1", "r1h2", "r1h3", "r1h4", "r1h5"},
		"r2": []string{"r2h1", "r2h2", "r2h3", "r2h4", "r2h5", "r2h6"},
		"r3": []string{"r3h1", "r3h2", "r3h3", "r3h4", "r3h5", "r3h6", "r3h7"},
		"r4": []string{"r4h1", "r4h2", "r4h3", "r4h4", "r4h5", "r4h6", "r4h7", "r4h8"},
	}
)

func (mockInventory) RackForHost(address string) (string, error) {
	for rack, hs := range mockInventoryMap {
		for _, host := range hs {
			if host == address {
				return rack, nil
			}
		}
	}
	return "", errors.New("can't find host")
}

func (mockInventory) Dump(pool string) []placement.Host {
	var hosts []placement.Host
	for rack, hs := range mockInventoryMap {
		for _, host := range hs {
			hosts = append(hosts, placement.Host{Address: host, Rack: rack})
		}
	}
	return hosts
}

type emptyInventory struct{}

func (emptyInventory) RackForHost(address string) (string, error) {
	return "", errors.New("can't find host")
}

func (emptyInventory) Dump(pool string) []placement.Host {
	return nil
}

type inventoryWithRackOneOnly struct{}

func (inventoryWithRackOneOnly) RackForHost(address string) (string, error) {
	for _, host := range mockInventoryMap["r1"] {
		if host == address {
			return "r1", nil
		}
	}
	return "", errors.New("can't find host")
}

func (inventoryWithRackOneOnly) Dump(pool string) []placement.Host {
	var hosts []placement.Host
	for _, host := range mockInventoryMap["r1"] {
		hosts = append(hosts, placement.Host{Address: host, Rack: "r1"})
	}
	return hosts
}

// file based snapshot storage
type mockStorage struct{}

const configFileSuffix = "_placement.json"

func getSnapshotFileName(service string) string {
	return service + configFileSuffix
}

func NewMockStorage() placement.SnapshotStorage {
	return mockStorage{}
}

func (ms mockStorage) SaveSnapshotForService(service string, p placement.Snapshot) error {
	var data []byte
	var err error
	if data, err = json.Marshal(p); err != nil {
		return err
	}
	return ioutil.WriteFile(getSnapshotFileName(service), data, 0644)
}

func (ms mockStorage) ReadSnapshotForService(service string) (placement.Snapshot, error) {
	var data []byte
	var err error
	if data, err = ioutil.ReadFile(getSnapshotFileName(service)); err != nil {
		return nil, err
	}
	return algo.NewPlacementFromJSON(data)
}
