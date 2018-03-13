// Copyright (c) 2017 Uber Technologies, Inc.
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

package selector

import (
	"errors"
	"fmt"
	"sort"

	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3cluster/placement/algo"
)

var (
	errInstanceAbsent  = errors.New("could not remove or replace a instance that does not exist")
	errNoValidInstance = errors.New("no valid instance in the candidate list")
)

type nonMirroredSelector struct {
	opts placement.Options
}

func newNonMirroredSelector(opts placement.Options) placement.InstanceSelector {
	return &nonMirroredSelector{opts: opts}
}

func (f *nonMirroredSelector) SelectInitialInstances(
	candidates []placement.Instance,
	rf int,
) ([]placement.Instance, error) {
	return getValidCandidates(placement.NewPlacement(), candidates, f.opts)
}

func (f *nonMirroredSelector) SelectAddingInstances(
	candidates []placement.Instance,
	p placement.Placement,
) ([]placement.Instance, error) {
	candidates, err := getValidCandidates(p, candidates, f.opts)
	if err != nil {
		return nil, err
	}

	candidateIGMap := buildIGMap(candidates)

	existingIGMap := buildIGMap(p.Instances())

	// If there is a isolation group not in the current placement, prefer the isolation group.
	for r, instances := range candidateIGMap {
		if _, exist := existingIGMap[r]; !exist {
			// All the isolation groups have at least 1 instance.
			return instances[:1], nil
		}
	}

	// Otherwise sort the isolation groups in the current placement
	// by capacity and find a instance from least sized isolation group.
	igs := make(sortableValues, 0, len(existingIGMap))
	for ig, instances := range existingIGMap {
		weight := 0
		for _, i := range instances {
			weight += int(i.Weight())
		}
		igs = append(igs, sortableValue{value: ig, weight: weight})
	}
	sort.Sort(igs)

	for _, ig := range igs {
		if i, exist := candidateIGMap[ig.value.(string)]; exist {
			for _, instance := range i {
				return []placement.Instance{instance}, nil
			}
		}
	}
	// no instance in the candidate instances can be added to the placement
	return nil, errNoValidInstance
}

func (f *nonMirroredSelector) SelectReplaceInstances(
	candidates []placement.Instance,
	leavingInstanceIDs []string,
	p placement.Placement,
) ([]placement.Instance, error) {
	candidates, err := getValidCandidates(p, candidates, f.opts)
	if err != nil {
		return nil, err
	}

	leavingInstances := make([]placement.Instance, 0, len(leavingInstanceIDs))
	for _, id := range leavingInstanceIDs {
		leavingInstance, exist := p.Instance(id)
		if !exist {
			return nil, errInstanceAbsent
		}
		leavingInstances = append(leavingInstances, leavingInstance)
	}

	// Map isolation group to instances.
	igMap := buildIGMap(candidates)

	// Otherwise sort the candidate instances by the number of conflicts.
	ph := algo.NewPlacementHelper(p, f.opts)
	instances := make([]sortableValue, 0, len(igMap))
	for ig, instancesInIG := range igMap {
		conflicts := 0
		for _, leaving := range leavingInstances {
			for _, s := range leaving.Shards().All() {
				if !ph.CanMoveShard(s.ID(), leaving, ig) {
					conflicts++
				}
			}
		}
		for _, instance := range instancesInIG {
			instances = append(instances, sortableValue{value: instance, weight: conflicts})
		}
	}

	groups := groupInstancesByConflict(instances, f.opts)
	if len(groups) == 0 {
		return nil, errNoValidInstance
	}

	var totalWeight uint32
	for _, instance := range leavingInstances {
		totalWeight += instance.Weight()
	}
	result, leftWeight := fillWeight(groups, int(totalWeight))

	if leftWeight > 0 && !f.opts.AllowPartialReplace() {
		return nil, fmt.Errorf("could not find enough instance to replace %v, %d weight could not be replaced",
			leavingInstances, leftWeight)
	}
	return result, nil
}

func groupInstancesByConflict(instancesSortedByConflicts []sortableValue, opts placement.Options) [][]placement.Instance {
	allowConflict := opts.AllowPartialReplace()
	sort.Sort(sortableValues(instancesSortedByConflicts))
	var groups [][]placement.Instance
	lastSeenConflict := -1
	for _, instance := range instancesSortedByConflicts {
		if !allowConflict && instance.weight > 0 {
			break
		}
		if instance.weight > lastSeenConflict {
			lastSeenConflict = instance.weight
			groups = append(groups, []placement.Instance{})
		}
		if lastSeenConflict == instance.weight {
			groups[len(groups)-1] = append(groups[len(groups)-1], instance.value.(placement.Instance))
		}
	}
	return groups
}

func fillWeight(groups [][]placement.Instance, targetWeight int) ([]placement.Instance, int) {
	var (
		result           []placement.Instance
		instancesInGroup []placement.Instance
	)
	for _, group := range groups {
		sort.Sort(placement.ByIDAscending(group))
		instancesInGroup, targetWeight = knapsack(group, targetWeight)
		result = append(result, instancesInGroup...)
		if targetWeight <= 0 {
			break
		}
	}
	return result, targetWeight
}

func knapsack(instances []placement.Instance, targetWeight int) ([]placement.Instance, int) {
	totalWeight := 0
	for _, instance := range instances {
		totalWeight += int(instance.Weight())
	}
	if totalWeight <= targetWeight {
		return instances[:], targetWeight - totalWeight
	}
	// totalWeight > targetWeight, there is a combination of instances to meet targetWeight for sure
	// we do dp until totalWeight rather than targetWeight here because we need to
	// at least cover the targetWeight, which is a little bit different than the knapsack problem
	weights := make([]int, totalWeight+1)
	combination := make([][]placement.Instance, totalWeight+1)

	// dp: weights[i][j] = max(weights[i-1][j], weights[i-1][j-instance.Weight] + instance.Weight)
	// when there are multiple combination to reach a target weight, we prefer the one with less instances
	for i := range instances {
		weight := int(instances[i].Weight())
		// this loop needs to go from len to 1 because weights is being updated in place
		for j := totalWeight; j >= 1; j-- {
			if j-weight < 0 {
				continue
			}
			newWeight := weights[j-weight] + weight
			if newWeight > weights[j] {
				weights[j] = weights[j-weight] + weight
				combination[j] = append(combination[j-weight], instances[i])
			} else if newWeight == weights[j] {
				// if can reach same weight, find a combination with less instances
				if len(combination[j-weight])+1 < len(combination[j]) {
					combination[j] = append(combination[j-weight], instances[i])
				}
			}
		}
	}

	for i := targetWeight; i <= totalWeight; i++ {
		if weights[i] >= targetWeight {
			return combination[i], targetWeight - weights[i]
		}
	}

	panic("should never reach here")
}

func buildIGMap(candidates []placement.Instance) map[string][]placement.Instance {
	result := make(map[string][]placement.Instance, len(candidates))
	for _, instance := range candidates {
		if _, exist := result[instance.IsolationGroup()]; !exist {
			result[instance.IsolationGroup()] = make([]placement.Instance, 0)
		}
		result[instance.IsolationGroup()] = append(result[instance.IsolationGroup()], instance)
	}
	return result
}

type sortableValue struct {
	value  interface{}
	weight int
}

type sortableValues []sortableValue

func (values sortableValues) Len() int {
	return len(values)
}

func (values sortableValues) Less(i, j int) bool {
	return values[i].weight < values[j].weight
}

func (values sortableValues) Swap(i, j int) {
	values[i], values[j] = values[j], values[i]
}
