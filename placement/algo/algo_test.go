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
	"fmt"
	"testing"

	"github.com/m3db/m3cluster/placement"
	"github.com/stretchr/testify/assert"
)

func TestGoodCase1(t *testing.T) {
	h1 := newHost("r1h1", "r1")
	h2 := newHost("r1h2", "r1")
	h3 := newHost("r2h3", "r2")
	h4 := newHost("r2h4", "r2")
	h5 := newHost("r3h5", "r3")
	h6 := newHost("r4h6", "r4")
	h7 := newHost("r5h7", "r5")
	h8 := newHost("r6h8", "r6")
	h9 := newHost("r7h9", "r7")

	hosts := []placement.Host{h1, h2, h3, h4, h5, h6, h7, h8, h9}

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := NewRackAwarePlacementAlgorithm()
	p, err := a.BuildInitialPlacement(hosts, ids)
	assert.NoError(t, err)
	validateDistribution(t, p.(placementSnapshot), 1.01, "good case1 replica 1")

	p, err = a.AddHost(p, newHost("r6h21", "r6"))
	assert.NoError(t, err)
	validateDistribution(t, p.(placementSnapshot), 1.01, "good case1 add 1")

	p, err = a.RemoveHost(p, h1)
	assert.NoError(t, err)
	validateDistribution(t, p.(placementSnapshot), 1.01, "good case1 remove 1")

	h12 := newHost("r3h12", "r3")
	p, err = a.ReplaceHost(p, h5, h12)
	assert.NoError(t, err)
	validateDistribution(t, p.(placementSnapshot), 1.01, "good case1 add 1")

	p, err = a.RemoveHost(p, h2)
	assert.NoError(t, err)
	validateDistribution(t, p.(placementSnapshot), 1.01, "good case1 remove 1")

	p, err = a.AddReplica(p)
	assert.NoError(t, err)
	validateDistribution(t, p.(placementSnapshot), 1.01, "good case1 replica 2")

	p, err = a.AddReplica(p)
	assert.NoError(t, err)
	validateDistribution(t, p.(placementSnapshot), 1.01, "good case1 replica 3")

	h10 := newHost("r4h10", "r4")
	p, err = a.AddHost(p, h10)
	assert.NoError(t, err)
	validateDistribution(t, p.(placementSnapshot), 1.01, "good case1 add 1")

	h11 := newHost("r7h11", "r7")
	p, err = a.AddHost(p, h11)
	assert.NoError(t, err)
	validateDistribution(t, p.(placementSnapshot), 1.01, "good case1 add 2")

	h13 := newHost("r5h13", "r5")
	p, err = a.ReplaceHost(p, h3, h13)
	assert.NoError(t, err)
	validateDistribution(t, p.(placementSnapshot), 1.01, "good case1 replace 1")

	p, err = a.RemoveHost(p, h4)
	assert.NoError(t, err)
	validateDistribution(t, p.(placementSnapshot), 1.02, "good case1 remove 2")
}

func TestOverSizedRack(t *testing.T) {
	r1h1 := newHost("r1h1", "r1")
	r1h6 := newHost("r1h6", "r1")
	r1h7 := newHost("r1h7", "r1")

	r2h2 := newHost("r2h2", "r2")
	r2h3 := newHost("r2h3", "r2")

	r3h4 := newHost("r3h4", "r3")
	r3h8 := newHost("r3h8", "r3")

	r4h5 := newHost("r4h5", "r4")

	r5h9 := newHost("r5h9", "r5")

	hosts := []placement.Host{r1h1, r2h2, r2h3, r3h4, r4h5, r1h6, r1h7, r3h8, r5h9}

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := rackAwarePlacementAlgorithm{}
	p, err := a.BuildInitialPlacement(hosts, ids)
	assert.NoError(t, err)
	validateDistribution(t, p.(placementSnapshot), 1.01, "TestOverSizedRack replica 1")

	p, err = a.AddReplica(p)
	assert.NoError(t, err)
	validateDistribution(t, p.(placementSnapshot), 1.01, "TestOverSizedRack replica 2")

	p, err = a.AddReplica(p)
	assert.NoError(t, err)
	validateDistribution(t, p.(placementSnapshot), 1.01, "TestOverSizedRack replica 3")

	r4h10 := newHost("r4h10", "r4")
	p, err = a.ReplaceHost(p, r3h8, r4h10)
	assert.NoError(t, err)
	validateDistribution(t, p.(placementSnapshot), 1.01, "TestOverSizedRack replace 1")

	//// At this point, r1 has 4 hosts to share a copy of 1024 partitions
	r1h11 := newHost("r1h11", "r1")
	p, err = a.ReplaceHost(p, r2h2, r1h11)
	assert.NoError(t, err)
	validateDistribution(t, p.(placementSnapshot), 1.22, "TestOverSizedRack replace 2")

	// adding a new host to relieve the load on the hot hosts
	r4h12 := newHost("r4h12", "r4")
	p, err = a.AddHost(p, r4h12)
	assert.NoError(t, err)
	validateDistribution(t, p.(placementSnapshot), 1.15, "TestOverSizedRack add 1")
}

func TestInitPlacementOn0Host(t *testing.T) {
	hosts := []placement.Host{}

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := rackAwarePlacementAlgorithm{}
	p, err := a.BuildInitialPlacement(hosts, ids)
	assert.Error(t, err)
	assert.Nil(t, p)
}

func TestOneRack(t *testing.T) {
	r1h1 := newHost("r1h1", "r1")
	r1h2 := newHost("r1h2", "r1")

	hosts := []placement.Host{r1h1, r1h2}

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := rackAwarePlacementAlgorithm{}
	p, err := a.BuildInitialPlacement(hosts, ids)
	assert.NoError(t, err)
	validateDistribution(t, p.(placementSnapshot), 1.01, "TestOneRack replica 1")

	r1h6 := newHost("r1h6", "r1")

	p, err = a.AddHost(p, r1h6)
	assert.NoError(t, err)
	validateDistribution(t, p.(placementSnapshot), 1.01, "TestOneRack addhost 1")
}

func TestRFGreaterThanRackLen(t *testing.T) {
	r1h1 := newHost("r1h1", "r1")
	r1h6 := newHost("r1h6", "r1")

	r2h2 := newHost("r2h2", "r2")
	r2h3 := newHost("r2h3", "r2")

	hosts := []placement.Host{r1h1, r2h2, r2h3, r1h6}

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := rackAwarePlacementAlgorithm{}
	p, err := a.BuildInitialPlacement(hosts, ids)
	assert.NoError(t, err)
	validateDistribution(t, p.(placementSnapshot), 1.01, "TestRFGreaterThanRackLen replica 1")

	p, err = a.AddReplica(p)
	assert.NoError(t, err)
	validateDistribution(t, p.(placementSnapshot), 1.01, "TestRFGreaterThanRackLen replica 2")

	p, err = a.AddReplica(p)
	assert.Error(t, err)
	assert.Nil(t, p)
}

func TestRFGreaterThanRackLenAfterHostRemoval(t *testing.T) {
	r1h1 := newHost("r1h1", "r1")

	r2h2 := newHost("r2h2", "r2")

	hosts := []placement.Host{r1h1, r2h2}

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := rackAwarePlacementAlgorithm{}
	p, err := a.BuildInitialPlacement(hosts, ids)
	assert.NoError(t, err)
	validateDistribution(t, p.(placementSnapshot), 1.01, "TestRFGreaterThanRackLenAfterHostRemoval replica 1")

	p, err = a.AddReplica(p)
	assert.NoError(t, err)
	validateDistribution(t, p.(placementSnapshot), 1.01, "TestRFGreaterThanRackLenAfterHostRemoval replica 2")

	p, err = a.RemoveHost(p, r2h2)
	assert.Error(t, err)
	assert.Nil(t, p)
}

func TestRFGreaterThanRackLenAfterHostReplace(t *testing.T) {
	r1h1 := newHost("r1h1", "r1")

	r2h2 := newHost("r2h2", "r2")

	hosts := []placement.Host{r1h1, r2h2}

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := rackAwarePlacementAlgorithm{}
	p, err := a.BuildInitialPlacement(hosts, ids)
	assert.NoError(t, err)
	validateDistribution(t, p.(placementSnapshot), 1.01, "TestRFGreaterThanRackLenAfterHostRemoval replica 1")

	p, err = a.AddReplica(p)
	assert.NoError(t, err)
	validateDistribution(t, p.(placementSnapshot), 1.01, "TestRFGreaterThanRackLenAfterHostRemoval replica 2")

	r1h3 := newHost("r1h3", "r1")
	p, err = a.ReplaceHost(p, r2h2, r1h3)
	assert.Error(t, err)
	assert.Nil(t, p)
}

func TestAddExistHost(t *testing.T) {
	r1h1 := newHost("r1h1", "r1")

	r2h2 := newHost("r2h2", "r2")

	hosts := []placement.Host{r1h1, r2h2}

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := rackAwarePlacementAlgorithm{}
	p, err := a.BuildInitialPlacement(hosts, ids)
	assert.NoError(t, err)
	validateDistribution(t, p.(placementSnapshot), 1.01, "TestAddExistHost replica 1")

	p, err = a.AddHost(p, r2h2)
	assert.Error(t, err)
	assert.Nil(t, p)
}

func TestRemoveAbsentHost(t *testing.T) {
	r1h1 := newHost("r1h1", "r1")

	r2h2 := newHost("r2h2", "r2")

	hosts := []placement.Host{r1h1, r2h2}

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := rackAwarePlacementAlgorithm{}
	p, err := a.BuildInitialPlacement(hosts, ids)
	assert.NoError(t, err)
	validateDistribution(t, p.(placementSnapshot), 1.01, "TestRemoveAbsentHost replica 1")

	r3h3 := newHost("r3h3", "r3")

	p, err = a.RemoveHost(p, r3h3)
	assert.Error(t, err)
	assert.Nil(t, p)
}

func TestReplaceAbsentHost(t *testing.T) {
	r1h1 := newHost("r1h1", "r1")

	r2h2 := newHost("r2h2", "r2")

	hosts := []placement.Host{r1h1, r2h2}

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := rackAwarePlacementAlgorithm{}
	p, err := a.BuildInitialPlacement(hosts, ids)
	assert.NoError(t, err)
	validateDistribution(t, p.(placementSnapshot), 1.01, "TestReplaceAbsentHost replica 1")

	r3h3 := newHost("r3h3", "r3")
	r4h4 := newHost("r4h4", "r4")

	p, err = a.ReplaceHost(p, r3h3, r4h4)
	assert.Error(t, err)
	assert.Nil(t, p)
}

func TestDeployment(t *testing.T) {
	h1 := newEmptyHostShards("r1h1", "r1")
	h1.addShard(1)
	h1.addShard(2)
	h1.addShard(3)

	h2 := newEmptyHostShards("r2h2", "r2")
	h2.addShard(4)
	h2.addShard(5)
	h2.addShard(6)

	h3 := newEmptyHostShards("r3h3", "r3")
	h3.addShard(1)
	h3.addShard(3)
	h3.addShard(5)

	h4 := newEmptyHostShards("r4h4", "r4")
	h4.addShard(2)
	h4.addShard(4)
	h4.addShard(6)

	h5 := newEmptyHostShards("r5h5", "r5")
	h5.addShard(5)
	h5.addShard(6)
	h5.addShard(1)

	h6 := newEmptyHostShards("r6h6", "r6")
	h6.addShard(2)
	h6.addShard(3)
	h6.addShard(4)

	hss := []*hostShards{h1, h2, h3, h4, h5, h6}

	mp := placementSnapshot{hostShards: hss, rf: 3, uniqueShards: []uint32{1, 2, 3, 4, 5, 6}}

	dp := newShardAwareDeploymentPlanner()
	steps := dp.DeploymentSteps(mp)
	total := 0
	for _, step := range steps {
		total += len(step)
	}
	assert.Equal(t, total, 6)
	assert.True(t, len(steps) == 3)

	algo := NewRackAwarePlacementAlgorithm()

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	// a more real case
	var hosts []placement.Host
	// 20 hosts from 10 racks
	for i := 1; i <= 20; i++ {
		hosts = append(hosts, newHost(fmt.Sprintf("r%vh%v", i/2, i), fmt.Sprintf("r%v", i/2)))
	}

	p, err := algo.BuildInitialPlacement(hosts, ids)
	assert.NoError(t, err)

	p, err = algo.AddReplica(p)
	assert.NoError(t, err)
	p, err = algo.AddReplica(p)
	assert.NoError(t, err)
	validateDistribution(t, p.(placementSnapshot), 1.01, "TestDeployment")

	steps = dp.DeploymentSteps(p)
	total = 0
	for _, step := range steps {
		total += len(step)
	}
	assert.Equal(t, total, 20)
	assert.True(t, len(steps) < len(hosts))

	hosts = hosts[:0]
	// The following case shows that the load on 1 host is pretty evenly replicated on other hosts.
	// So that no 2 hosts can be deployed together because of the limited length of shards
	for i := 1; i <= 20; i++ {
		hosts = append(hosts, newHost(fmt.Sprintf("r%vh%v", i, i), fmt.Sprintf("r%v", i)))
	}

	p, err = algo.BuildInitialPlacement(hosts, ids)
	assert.NoError(t, err)

	p, err = algo.AddReplica(p)
	assert.NoError(t, err)
	p, err = algo.AddReplica(p)
	assert.NoError(t, err)
	validateDistribution(t, p.(placementSnapshot), 1.02, "TestDeployment")

	steps = dp.DeploymentSteps(p)
	total = 0
	for _, step := range steps {
		total += len(step)
	}
	assert.Equal(t, total, 20)
	assert.True(t, len(steps) == len(hosts))
}

func TestCanAssignHost(t *testing.T) {
	h1 := newEmptyHostShards("r1h1", "r1")
	h1.addShard(1)
	h1.addShard(2)
	h1.addShard(3)

	h2 := newEmptyHostShards("r1h2", "r1")
	h2.addShard(4)
	h2.addShard(5)
	h2.addShard(6)

	h3 := newEmptyHostShards("r2h3", "r2")
	h3.addShard(1)
	h3.addShard(3)
	h3.addShard(5)

	h4 := newEmptyHostShards("r2h4", "r2")
	h4.addShard(2)
	h4.addShard(4)
	h4.addShard(6)

	h5 := newEmptyHostShards("r3h5", "r3")
	h5.addShard(5)
	h5.addShard(6)
	h5.addShard(1)

	h6 := newEmptyHostShards("r4h6", "r4")
	h6.addShard(2)
	h6.addShard(3)
	h6.addShard(4)

	hss := []*hostShards{h1, h2, h3, h4, h5, h6}

	mp := placementSnapshot{hostShards: hss, rf: 3, uniqueShards: []uint32{1, 2, 3, 4, 5, 6}}

	ph := newReplicaPlacementHelper(mp, 3)
	assert.True(t, ph.canAssignHost(2, h6, h5))
	assert.True(t, ph.canAssignHost(1, h1, h6))
	assert.False(t, ph.canAssignHost(2, h6, h1))
	assert.False(t, ph.canAssignHost(2, h6, h3))
}

func validateDistribution(t *testing.T, mp placementSnapshot, expectPeakOverAvg float64, testCase string) {
	sh := newPlaceShardingHelper(mp, mp.Replicas(), true)
	total := 0

	for _, hostShard := range mp.hostShards {
		hostLoad := hostShard.shardLen()
		total += hostLoad
		hostOverAvg := float64(hostLoad) / float64(getAvgLoad(sh))
		assert.True(t, hostOverAvg <= expectPeakOverAvg, fmt.Sprintf("Bad distribution in %s, peak/Avg on %s is too high: %v, expecting %v, load on host: %v, avg load: %v",
			testCase, hostShard.hostAddress(), hostOverAvg, expectPeakOverAvg, hostLoad, getAvgLoad(sh)))

		target := sh.hostHeap.getTargetLoadForHost(hostShard.hostAddress())
		hostOverTarget := float64(hostLoad) / float64(target)
		assert.True(t, hostOverTarget <= 1.03, fmt.Sprintf("Bad distribution in %s, peak/Target is too high. %s: %v, load on host: %v, target load: %v",
			testCase, hostShard.hostAddress(), hostOverTarget, hostLoad, target))
	}
	assert.Equal(t, total, mp.rf*mp.ShardsLen(), fmt.Sprintf("Wrong total partition: expecting %v, but got %v", mp.rf*mp.ShardsLen(), total))
}

func getAvgLoad(ph *placementHelper) int {
	totalLoad := ph.rf * ph.getShardLen()
	numberOfHosts := ph.getHostLen()
	return totalLoad / numberOfHosts
}
