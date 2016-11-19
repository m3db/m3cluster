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
	"math/rand"
	"sort"
	"testing"

	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/placement"
	"github.com/stretchr/testify/assert"
)

func testQuery() services.ServiceQuery {
	return services.NewServiceQuery().SetService("test_service")
}

func TestGoodWorkflow(t *testing.T) {
	p := NewPlacementService(NewMockStorage(), testQuery(), placement.NewOptions())
	testGoodWorkflow(t, p)

	p = NewPlacementService(NewMockStorage(), testQuery(), placement.NewOptions().SetLooseRackCheck(true))
	testGoodWorkflow(t, p)
}

func testGoodWorkflow(t *testing.T, p services.PlacementService) {
	h1 := placement.NewEmptyInstance("r1h1", "r1", "z1", 2)
	h2 := placement.NewEmptyInstance("r2h2", "r2", "z1", 2)
	h3 := placement.NewEmptyInstance("r3h3", "r3", "z1", 2)
	_, err := p.BuildInitialPlacement([]services.PlacementInstance{h1, h2}, 10, 1)
	assert.NoError(t, err)

	_, err = p.AddReplica()
	assert.NoError(t, err)

	_, err = p.AddInstance([]services.PlacementInstance{h3})
	assert.NoError(t, err)

	_, err = p.RemoveInstance(h1)
	assert.NoError(t, err)

	_, err = p.ReplaceInstance(
		h2,
		[]services.PlacementInstance{
			placement.NewEmptyInstance("h21", "r2", "z1", 1),
			placement.NewEmptyInstance("h4", "r4", "z1", 1),
			h3, // already in placement
			placement.NewEmptyInstance("h31", "r3", "z1", 1), // conflict
		},
	)
	assert.NoError(t, err)
	s, err := p.Placement()
	assert.NoError(t, err)
	assert.Equal(t, 3, s.NumInstances())
	assert.NotNil(t, s.Instance("h21"))
	assert.NotNil(t, s.Instance("h4"))

	_, err = p.AddInstance([]services.PlacementInstance{h1})
	assert.NoError(t, err)

	_, err = p.AddInstance([]services.PlacementInstance{placement.NewEmptyInstance("r2h4", "r2", "z1", 1)})
	assert.NoError(t, err)

	_, err = p.AddInstance([]services.PlacementInstance{placement.NewEmptyInstance("r3h4", "r3", "z1", 1)})
	assert.NoError(t, err)
	_, err = p.AddInstance([]services.PlacementInstance{placement.NewEmptyInstance("r3h5", "r3", "z1", 1)})
	assert.NoError(t, err)

	instances := []services.PlacementInstance{
		placement.NewEmptyInstance("r1h5", "r1", "z1", 1),
		placement.NewEmptyInstance("r3h4", "r3", "z1", 1),
		placement.NewEmptyInstance("r3h5", "r3", "z1", 1),
		placement.NewEmptyInstance("r3h6", "r3", "z1", 1),
		placement.NewEmptyInstance("r2h3", "r2", "z1", 1),
		placement.NewEmptyInstance("r4h41", "r4", "z1", 1),
	}
	_, err = p.AddInstance(instances)
	assert.NoError(t, err)
	s, err = p.Placement()
	assert.NoError(t, err)
	assert.NotNil(t, s.Instance("r4h41")) // instance added from least weighted rack
}

func TestBadInitialPlacement(t *testing.T) {
	p := NewPlacementService(NewMockStorage(), testQuery(), placement.NewOptions())

	// not enough instances
	_, err := p.BuildInitialPlacement([]services.PlacementInstance{}, 10, 1)
	assert.Error(t, err)

	// not enough racks
	_, err = p.BuildInitialPlacement([]services.PlacementInstance{
		placement.NewEmptyInstance("r1h1", "r1", "z1", 1),
		placement.NewEmptyInstance("r1h2", "r1", "z1", 1),
	}, 100, 2)
	assert.Error(t, err)

	// too many zones
	_, err = p.BuildInitialPlacement([]services.PlacementInstance{
		placement.NewEmptyInstance("r1h1", "r1", "z1", 1),
		placement.NewEmptyInstance("r2h2", "r2", "z2", 1),
	}, 100, 2)
	assert.Error(t, err)
	assert.Equal(t, errMultipleZones, err)

	_, err = p.BuildInitialPlacement([]services.PlacementInstance{
		placement.NewEmptyInstance("r1h1", "r1", "z1", 1),
		placement.NewEmptyInstance("r2h2", "r2", "z1", 1),
	}, 100, 2)
	assert.NoError(t, err)

	_, err = p.BuildInitialPlacement([]services.PlacementInstance{
		placement.NewEmptyInstance("r1h1", "r1", "z1", 1),
		placement.NewEmptyInstance("r2h2", "r2", "z1", 1),
	}, 100, 2)
	assert.Error(t, err)
	assert.Equal(t, errPlacementAlreadyExist, err)
}

func TestBadAddReplica(t *testing.T) {
	p := NewPlacementService(NewMockStorage(), testQuery(), placement.NewOptions())

	_, err := p.BuildInitialPlacement([]services.PlacementInstance{placement.NewEmptyInstance("r1h1", "r1", "z1", 1)}, 10, 1)
	assert.NoError(t, err)

	// not enough racks/instances
	_, err = p.AddReplica()
	assert.Error(t, err)

	// could not find placement for service
	p = NewPlacementService(NewMockStorage(), testQuery(), placement.NewOptions())
	_, err = p.AddReplica()
	assert.Error(t, err)
}

func TestBadAddInstance(t *testing.T) {
	ms := NewMockStorage()
	p := NewPlacementService(ms, testQuery(), placement.NewOptions())

	_, err := p.BuildInitialPlacement([]services.PlacementInstance{placement.NewEmptyInstance("r1h1", "r1", "z1", 1)}, 10, 1)
	assert.NoError(t, err)

	// adding instance already exist
	_, err = p.AddInstance([]services.PlacementInstance{placement.NewEmptyInstance("r1h1", "r1", "z1", 1)})
	assert.Error(t, err)

	// too many zones
	_, err = p.AddInstance([]services.PlacementInstance{placement.NewEmptyInstance("r2h2", "r2", "z2", 1)})
	assert.Error(t, err)
	assert.Equal(t, errNoValidInstance, err)

	// algo error
	psWithErrorAlgo := placementService{algo: errorAlgorithm{}, ss: ms, service: testQuery(), options: placement.NewOptions()}
	_, err = psWithErrorAlgo.AddInstance([]services.PlacementInstance{placement.NewEmptyInstance("r2h2", "r2", "z1", 1)})
	assert.Error(t, err)

	p = NewPlacementService(ms, testQuery(), placement.NewOptions())
	_, err = p.AddInstance([]services.PlacementInstance{placement.NewEmptyInstance("r1h1", "r1", "z1", 1)})
	assert.Error(t, err)

	// could not find placement for service
	p = NewPlacementService(NewMockStorage(), testQuery(), placement.NewOptions())
	_, err = p.AddInstance([]services.PlacementInstance{placement.NewEmptyInstance("r2h2", "r2", "z1", 1)})
	assert.Error(t, err)
}

func TestBadRemoveInstance(t *testing.T) {
	p := NewPlacementService(NewMockStorage(), testQuery(), placement.NewOptions())

	_, err := p.BuildInitialPlacement([]services.PlacementInstance{placement.NewEmptyInstance("r1h1", "r1", "z1", 1)}, 10, 1)
	assert.NoError(t, err)

	// leaving instance not exist
	_, err = p.RemoveInstance(placement.NewEmptyInstance("r2h2", "r2", "z1", 1))
	assert.Error(t, err)

	// not enough racks/instances after removal
	_, err = p.RemoveInstance(placement.NewEmptyInstance("r1h1", "r1", "z1", 1))
	assert.Error(t, err)

	// could not find placement for service
	p = NewPlacementService(NewMockStorage(), testQuery(), placement.NewOptions())
	_, err = p.RemoveInstance(placement.NewEmptyInstance("r1h1", "r1", "z1", 1))
	assert.Error(t, err)
}

func TestBadReplaceInstance(t *testing.T) {
	p := NewPlacementService(NewMockStorage(), testQuery(), placement.NewOptions())

	_, err := p.BuildInitialPlacement([]services.PlacementInstance{
		placement.NewEmptyInstance("r1h1", "r1", "z1", 1),
		placement.NewEmptyInstance("r4h4", "r4", "z1", 1),
	}, 10, 1)
	assert.NoError(t, err)

	// leaving instance not exist
	_, err = p.ReplaceInstance(

		placement.NewEmptyInstance("r1h2", "r1", "z1", 1),
		[]services.PlacementInstance{placement.NewEmptyInstance("r2h2", "r2", "z1", 1)},
	)
	assert.Error(t, err)

	// adding instance already exist
	_, err = p.ReplaceInstance(

		placement.NewEmptyInstance("r1h1", "r1", "z1", 1),
		[]services.PlacementInstance{placement.NewEmptyInstance("r4h4", "r4", "z1", 1)},
	)
	assert.Error(t, err)

	// not enough rack after replace
	_, err = p.AddReplica()
	assert.NoError(t, err)
	_, err = p.ReplaceInstance(

		placement.NewEmptyInstance("r4h4", "r4", "z1", 1),
		[]services.PlacementInstance{placement.NewEmptyInstance("r1h2", "r1", "z1", 1)},
	)
	assert.Error(t, err)

	// catch algo errors
	psWithErrorAlgo := placementService{algo: errorAlgorithm{}, ss: NewMockStorage(), service: testQuery(), options: placement.NewOptions()}
	_, err = psWithErrorAlgo.ReplaceInstance(

		placement.NewEmptyInstance("r1h1", "r1", "z1", 1),
		[]services.PlacementInstance{placement.NewEmptyInstance("r2h2", "r2", "z1", 1)},
	)
	assert.Error(t, err)

	// could not find placement for service
	p = NewPlacementService(NewMockStorage(), testQuery(), placement.NewOptions())
	_, err = p.ReplaceInstance(
		placement.NewEmptyInstance("r1h1", "r1", "z1", 1),
		[]services.PlacementInstance{placement.NewEmptyInstance("r2h2", "r2", "z1", 1)},
	)
	assert.Error(t, err)
}

func TestReplaceInstanceWithLooseRackCheck(t *testing.T) {
	p := NewPlacementService(NewMockStorage(), testQuery(), placement.NewOptions().SetLooseRackCheck(true))

	_, err := p.BuildInitialPlacement(

		[]services.PlacementInstance{
			placement.NewEmptyInstance("r1h1", "r1", "z1", 1),
			placement.NewEmptyInstance("r4h4", "r4", "z1", 1),
		}, 10, 1)
	assert.NoError(t, err)

	// leaving instance not exist
	_, err = p.ReplaceInstance(

		placement.NewEmptyInstance("r1h2", "r1", "z1", 1),
		[]services.PlacementInstance{placement.NewEmptyInstance("r2h2", "r2", "z1", 1)},
	)
	assert.Error(t, err)

	// adding instance already exist
	_, err = p.ReplaceInstance(

		placement.NewEmptyInstance("r1h1", "r1", "z1", 1),
		[]services.PlacementInstance{placement.NewEmptyInstance("r4h4", "r4", "z1", 1)},
	)
	assert.Error(t, err)

	// NO ERROR when not enough rack after replace
	_, err = p.AddReplica()
	assert.NoError(t, err)
	_, err = p.ReplaceInstance(

		placement.NewEmptyInstance("r4h4", "r4", "z1", 1),
		[]services.PlacementInstance{placement.NewEmptyInstance("r1h2", "r1", "z1", 1)},
	)
	assert.NoError(t, err)

	// could not find placement for service
	p = NewPlacementService(NewMockStorage(), testQuery(), placement.NewOptions())
	_, err = p.ReplaceInstance(
		placement.NewEmptyInstance("r1h1", "r1", "z1", 1),
		[]services.PlacementInstance{placement.NewEmptyInstance("r2h2", "r2", "z1", 1)},
	)
	assert.Error(t, err)
}

func TestFindReplaceInstance(t *testing.T) {
	h1 := placement.NewEmptyInstance("r1h1", "r11", "z1", 1)
	h1.Shards().AddShard(1)
	h1.Shards().AddShard(2)
	h1.Shards().AddShard(3)

	h10 := placement.NewEmptyInstance("r1h10", "r11", "z1", 1)
	h10.Shards().AddShard(4)
	h10.Shards().AddShard(5)

	h2 := placement.NewEmptyInstance("r2h2", "r12", "z1", 1)
	h2.Shards().AddShard(6)
	h2.Shards().AddShard(7)
	h2.Shards().AddShard(8)
	h2.Shards().AddShard(9)

	h3 := placement.NewEmptyInstance("r3h3", "r13", "z1", 3)
	h3.Shards().AddShard(1)
	h3.Shards().AddShard(3)
	h3.Shards().AddShard(4)
	h3.Shards().AddShard(5)
	h3.Shards().AddShard(6)

	h4 := placement.NewEmptyInstance("r4h4", "r14", "z1", 1)
	h4.Shards().AddShard(2)
	h4.Shards().AddShard(7)
	h4.Shards().AddShard(8)
	h4.Shards().AddShard(9)

	instances := []services.PlacementInstance{h1, h2, h3, h4, h10}

	ids := []uint32{1, 2, 3, 4, 5, 6, 7, 8}
	s := placement.NewPlacement(instances, ids, 2)

	candidates := []services.PlacementInstance{
		placement.NewEmptyInstance("h11", "r11", "z1", 1),
		placement.NewEmptyInstance("h22", "r22", "z2", 1), // bad zone
	}

	p := NewPlacementService(NewMockStorage(), testQuery(), placement.NewOptions()).(placementService)
	i, err := p.findReplaceInstance(s, candidates, h4)
	assert.Error(t, err)
	assert.Nil(t, i)

	noConflictCandidates := []services.PlacementInstance{
		placement.NewEmptyInstance("h11", "r0", "z1", 1),
		placement.NewEmptyInstance("h22", "r0", "z2", 1),
	}
	i, err = p.findReplaceInstance(s, noConflictCandidates, h3)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "could not find enough instance to replace")
	assert.Nil(t, i)

	p = NewPlacementService(NewMockStorage(), testQuery(), placement.NewOptions().SetLooseRackCheck(true)).(placementService)
	i, err = p.findReplaceInstance(s, candidates, h4)
	assert.NoError(t, err)
	// gonna prefer r1 because r1 would only conflict shard 2, r2 would conflict 7,8,9
	assert.Equal(t, 1, len(i))
	assert.Equal(t, "r11", i[0].Rack())
}

func TestGroupInstancesByConflict(t *testing.T) {
	h1 := placement.NewEmptyInstance("h1", "", "", 1)
	h2 := placement.NewEmptyInstance("h2", "", "", 1)
	h3 := placement.NewEmptyInstance("h3", "", "", 1)
	h4 := placement.NewEmptyInstance("h4", "", "", 2)
	instanceConflicts := []sortableValue{
		sortableValue{value: h1, weight: 1},
		sortableValue{value: h2, weight: 0},
		sortableValue{value: h3, weight: 3},
		sortableValue{value: h4, weight: 2},
	}

	groups := groupInstancesByConflict(instanceConflicts, true)
	assert.Equal(t, 4, len(groups))
	assert.Equal(t, h2, groups[0][0])
	assert.Equal(t, h1, groups[1][0])
	assert.Equal(t, h4, groups[2][0])
	assert.Equal(t, h3, groups[3][0])

	groups = groupInstancesByConflict(instanceConflicts, false)
	assert.Equal(t, 1, len(groups))
	assert.Equal(t, h2, groups[0][0])
}

func TestKnapSack(t *testing.T) {
	h1 := placement.NewEmptyInstance("h1", "", "", 40000)
	h2 := placement.NewEmptyInstance("h2", "", "", 20000)
	h3 := placement.NewEmptyInstance("h3", "", "", 80000)
	h4 := placement.NewEmptyInstance("h4", "", "", 50000)
	h5 := placement.NewEmptyInstance("h5", "", "", 190000)
	instances := []services.PlacementInstance{h1, h2, h3, h4, h5}

	res, leftWeight := knapsack(instances, 10000)
	assert.Equal(t, -10000, leftWeight)
	assert.Equal(t, []services.PlacementInstance{h2}, res)

	res, leftWeight = knapsack(instances, 20000)
	assert.Equal(t, 0, leftWeight)
	assert.Equal(t, []services.PlacementInstance{h2}, res)

	res, leftWeight = knapsack(instances, 30000)
	assert.Equal(t, -10000, leftWeight)
	assert.Equal(t, []services.PlacementInstance{h1}, res)

	res, leftWeight = knapsack(instances, 60000)
	assert.Equal(t, 0, leftWeight)
	assert.Equal(t, []services.PlacementInstance{h1, h2}, res)

	res, leftWeight = knapsack(instances, 120000)
	assert.Equal(t, 0, leftWeight)
	assert.Equal(t, []services.PlacementInstance{h1, h3}, res)

	res, leftWeight = knapsack(instances, 170000)
	assert.Equal(t, 0, leftWeight)
	assert.Equal(t, []services.PlacementInstance{h1, h3, h4}, res)

	res, leftWeight = knapsack(instances, 190000)
	assert.Equal(t, 0, leftWeight)
	// will prefer h5 than h1+h2+h3+h4
	assert.Equal(t, []services.PlacementInstance{h5}, res)

	res, leftWeight = knapsack(instances, 200000)
	assert.Equal(t, -10000, leftWeight)
	assert.Equal(t, []services.PlacementInstance{h2, h5}, res)

	res, leftWeight = knapsack(instances, 210000)
	assert.Equal(t, 0, leftWeight)
	assert.Equal(t, []services.PlacementInstance{h2, h5}, res)

	res, leftWeight = knapsack(instances, 400000)
	assert.Equal(t, 20000, leftWeight)
	assert.Equal(t, []services.PlacementInstance{h1, h2, h3, h4, h5}, res)
}

func TestFillWeight(t *testing.T) {
	h1 := placement.NewEmptyInstance("h1", "", "", 4)
	h2 := placement.NewEmptyInstance("h2", "", "", 2)
	h3 := placement.NewEmptyInstance("h3", "", "", 8)
	h4 := placement.NewEmptyInstance("h4", "", "", 5)
	h5 := placement.NewEmptyInstance("h5", "", "", 19)

	h6 := placement.NewEmptyInstance("h6", "", "", 3)
	h7 := placement.NewEmptyInstance("h7", "", "", 7)
	groups := [][]services.PlacementInstance{
		[]services.PlacementInstance{h1, h2, h3, h4, h5},
		[]services.PlacementInstance{h6, h7},
	}

	// When targetWeight is smaller than 38, the first group will satisfy
	res, leftWeight := fillWeight(groups, 1)
	assert.Equal(t, -1, leftWeight)
	assert.Equal(t, []services.PlacementInstance{h2}, res)

	res, leftWeight = fillWeight(groups, 2)
	assert.Equal(t, 0, leftWeight)
	assert.Equal(t, []services.PlacementInstance{h2}, res)

	res, leftWeight = fillWeight(groups, 17)
	assert.Equal(t, 0, leftWeight)
	assert.Equal(t, []services.PlacementInstance{h1, h3, h4}, res)

	res, leftWeight = fillWeight(groups, 20)
	assert.Equal(t, -1, leftWeight)
	assert.Equal(t, []services.PlacementInstance{h2, h5}, res)

	// When targetWeight is bigger than 38, need to get instance from group 2
	res, leftWeight = fillWeight(groups, 40)
	assert.Equal(t, -1, leftWeight)
	assert.Equal(t, []services.PlacementInstance{h1, h2, h3, h4, h5, h6}, res)

	res, leftWeight = fillWeight(groups, 41)
	assert.Equal(t, 0, leftWeight)
	assert.Equal(t, []services.PlacementInstance{h1, h2, h3, h4, h5, h6}, res)

	res, leftWeight = fillWeight(groups, 47)
	assert.Equal(t, -1, leftWeight)
	assert.Equal(t, []services.PlacementInstance{h1, h2, h3, h4, h5, h6, h7}, res)

	res, leftWeight = fillWeight(groups, 48)
	assert.Equal(t, 0, leftWeight)
	assert.Equal(t, []services.PlacementInstance{h1, h2, h3, h4, h5, h6, h7}, res)

	res, leftWeight = fillWeight(groups, 50)
	assert.Equal(t, 2, leftWeight)
	assert.Equal(t, []services.PlacementInstance{h1, h2, h3, h4, h5, h6, h7}, res)
}

func TestFillWeightDeterministic(t *testing.T) {
	h1 := placement.NewEmptyInstance("h1", "", "", 1)
	h2 := placement.NewEmptyInstance("h2", "", "", 1)
	h3 := placement.NewEmptyInstance("h3", "", "", 1)
	h4 := placement.NewEmptyInstance("h4", "", "", 3)
	h5 := placement.NewEmptyInstance("h5", "", "", 4)

	h6 := placement.NewEmptyInstance("h6", "", "", 1)
	h7 := placement.NewEmptyInstance("h7", "", "", 1)
	h8 := placement.NewEmptyInstance("h8", "", "", 1)
	h9 := placement.NewEmptyInstance("h9", "", "", 2)
	groups := [][]services.PlacementInstance{
		[]services.PlacementInstance{h1, h2, h3, h4, h5},
		[]services.PlacementInstance{h6, h7, h8, h9},
	}

	for i := 1; i < 17; i++ {
		testResultDeterministic(t, groups, i)
	}
}

func testResultDeterministic(t *testing.T, groups [][]services.PlacementInstance, targetWeight int) {
	res, _ := fillWeight(groups, targetWeight)

	// shuffle the order of of each group of instances
	for _, group := range groups {
		for i := range group {
			j := rand.Intn(i + 1)
			group[i], group[j] = group[j], group[i]
		}
	}
	res1, _ := fillWeight(groups, targetWeight)
	assert.Equal(t, res, res1)
}

func TestRackLenSort(t *testing.T) {
	r1 := sortableValue{value: "r1", weight: 1}
	r2 := sortableValue{value: "r2", weight: 2}
	r3 := sortableValue{value: "r3", weight: 3}
	r4 := sortableValue{value: "r4", weight: 2}
	r5 := sortableValue{value: "r5", weight: 1}
	r6 := sortableValue{value: "r6", weight: 2}
	r7 := sortableValue{value: "r7", weight: 3}
	rs := sortableThings{r1, r2, r3, r4, r5, r6, r7}
	sort.Sort(rs)

	seen := 0
	for _, rl := range rs {
		assert.True(t, seen <= rl.weight)
		seen = rl.weight
	}
}

type errorAlgorithm struct{}

func (errorAlgorithm) InitialPlacement(instances []services.PlacementInstance, ids []uint32) (services.ServicePlacement, error) {
	return nil, errors.New("error in errorAlgorithm")
}

func (errorAlgorithm) AddReplica(p services.ServicePlacement) (services.ServicePlacement, error) {
	return nil, errors.New("error in errorAlgorithm")
}

func (errorAlgorithm) AddInstance(p services.ServicePlacement, h services.PlacementInstance) (services.ServicePlacement, error) {
	return nil, errors.New("error in errorAlgorithm")
}

func (errorAlgorithm) RemoveInstance(p services.ServicePlacement, h services.PlacementInstance) (services.ServicePlacement, error) {
	return nil, errors.New("error in errorAlgorithm")
}

func (errorAlgorithm) ReplaceInstance(p services.ServicePlacement, leavingInstance services.PlacementInstance, addingInstance []services.PlacementInstance) (services.ServicePlacement, error) {
	return nil, errors.New("error in errorAlgorithm")
}

// file based placement storage
type mockStorage struct {
	m map[string]services.ServicePlacement
}

func NewMockStorage() placement.Storage {
	return &mockStorage{m: map[string]services.ServicePlacement{}}
}

func (ms *mockStorage) SetPlacement(service services.ServiceQuery, p services.ServicePlacement) error {
	var err error
	if err = p.Validate(); err != nil {
		return err
	}
	ms.m[service.Service()] = p
	return nil
}

func (ms *mockStorage) Placement(service services.ServiceQuery) (services.ServicePlacement, error) {
	if p, exist := ms.m[service.Service()]; exist {
		return p, nil
	}
	return nil, errors.New("could not find placement for service")
}
