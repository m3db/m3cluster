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
	"testing"

	"github.com/m3db/m3cluster/placement"
	"github.com/stretchr/testify/assert"
)

func TestGeneratingConfig(t *testing.T) {
	h1 := newHost("r1h1", "r1")
	h3 := newHost("r2h3", "r2")
	h4 := newHost("r2h4", "r2")
	h5 := newHost("r3h5", "r3")
	h6 := newHost("r4h6", "r4")
	h7 := newHost("r5h7", "r5")
	h9 := newHost("r6h9", "r6")

	hosts := []placement.Host{h1, h3, h4, h5, h6, h7, h9}

	ids := make([]uint32, 20)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := NewRackAwarePlacementAlgorithm()
	p, err := a.BuildInitialPlacement(hosts, ids)
	assert.NoError(t, err)
	testSnapshotJSONRoundTrip(t, p.(placementSnapshot))

	p, err = a.AddReplica(p)
	assert.NoError(t, err)
	testSnapshotJSONRoundTrip(t, p.(placementSnapshot))

	p, err = a.AddReplica(p)
	assert.NoError(t, err)
	testSnapshotJSONRoundTrip(t, p.(placementSnapshot))

	p, err = a.RemoveHost(p, h4)
	assert.NoError(t, err)
	testSnapshotJSONRoundTrip(t, p.(placementSnapshot))

	h10 := newHost("r4h10", "r4")
	a.AddHost(p, h10)
	assert.NoError(t, err)
	testSnapshotJSONRoundTrip(t, p.(placementSnapshot))

	h11 := newHost("r3h11", "r3")
	a.AddHost(p, h11)
	assert.NoError(t, err)
	testSnapshotJSONRoundTrip(t, p.(placementSnapshot))

	h12 := newHost("r5h11", "r5")
	a.ReplaceHost(p, h1, h12)
	assert.NoError(t, err)
	testSnapshotJSONRoundTrip(t, p.(placementSnapshot))

}

func TestSnapshotMarshalling(t *testing.T) {
	invalidJSON := `[
		{"Address":123,"Rack":"r1","Shards":[0,7,11]}
	]`
	data := []byte(invalidJSON)
	ps, err := NewPlacementFromJSON(data)
	assert.Nil(t, ps)
	assert.Error(t, err)

	validJSON := `[
		{"Address":"r1h1","Rack":"r1","Shards":[0,7,11]},
		{"Address":"r2h3","Rack":"r2","Shards":[1,4,12]},
		{"Address":"r2h4","Rack":"r2","Shards":[6,13,15]},
		{"Address":"r3h5","Rack":"r3","Shards":[2,8,19]},
		{"Address":"r4h6","Rack":"r4","Shards":[3,9,18]},
		{"Address":"r5h7","Rack":"r5","Shards":[10,14]},
		{"Address":"r6h9","Rack":"r6","Shards":[5,16,17]}
	]`
	data = []byte(validJSON)
	ps, err = NewPlacementFromJSON(data)
	assert.NoError(t, err)
	assert.Equal(t, 7, ps.HostsLen())
	assert.Equal(t, 1, ps.Replicas())
	assert.Equal(t, 20, ps.ShardsLen())

	// an extra replica for shard 1
	invalidPlacementJSON := `[
		{"Address":"r1h1","Rack":"r1","Shards":[0,1,7,11]},
		{"Address":"r2h3","Rack":"r2","Shards":[1,4,12]},
		{"Address":"r2h4","Rack":"r2","Shards":[6,13,15]},
		{"Address":"r3h5","Rack":"r3","Shards":[2,8,19]},
		{"Address":"r4h6","Rack":"r4","Shards":[3,9,18]},
		{"Address":"r5h7","Rack":"r5","Shards":[10,14]},
		{"Address":"r6h9","Rack":"r6","Shards":[5,16,17]}
	]`
	data = []byte(invalidPlacementJSON)
	ps, err = NewPlacementFromJSON(data)
	assert.Equal(t, err, errShardsWithDifferentReplicas)
	assert.Nil(t, ps)
}

func testSnapshotJSONRoundTrip(t *testing.T, snapShot placementSnapshot) {
	json1, err := json.Marshal(snapShot)
	assert.NoError(t, err)

	var snapShotFromJSON placementSnapshot
	err = json.Unmarshal(json1, &snapShotFromJSON)
	assert.NoError(t, err)

	json2, err := json.Marshal(snapShot)
	assert.NoError(t, err)
	assert.Equal(t, string(json1), string(json2))
}
