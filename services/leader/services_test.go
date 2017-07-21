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

package leader

import (
	"testing"

	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/m3db/m3cluster/services"
	"github.com/stretchr/testify/assert"
)

func overrideOpts(s string) services.CampaignOptions {
	return services.NewCampaignOptions().SetLeaderValue(s)
}

func TestNewService(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc, err := NewService(tc.etcdClient(), tc.options())
	assert.NoError(t, err)
	assert.NotNil(t, svc)
}

func TestService_Campaign(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc := tc.service()

	sc, err := svc.Campaign("", overrideOpts("foo1"))
	assert.NoError(t, err)
	assert.NoError(t, waitForStates(sc, true, followerS, leaderS))

	sc2, err := svc.Campaign("1", overrideOpts("foo1"))
	assert.NoError(t, err)
	assert.NoError(t, waitForStates(sc2, true, followerS, leaderS))

	for _, eid := range []string{"", "1"} {
		ld, err := svc.Leader(eid)
		assert.NoError(t, err)
		assert.Equal(t, "foo1", ld)
	}
}

func TestService_Resign(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc := tc.service()
	sc, err := svc.Campaign("e", overrideOpts("foo1"))
	assert.NoError(t, err)
	assert.NoError(t, waitForStates(sc, true, followerS, leaderS))

	err = svc.Resign("zzz")
	assert.Error(t, err)

	err = svc.Resign("e")
	assert.NoError(t, err)
	assert.NoError(t, waitForStates(sc, false, followerS))
}

func TestService_Leader(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc := tc.service()

	sc, err := svc.Campaign("", overrideOpts("foo1"))
	assert.NoError(t, err)
	assert.NoError(t, waitForStates(sc, true, followerS, leaderS))

	_, err = svc.Leader("z")
	assert.Equal(t, concurrency.ErrElectionNoLeader, err)

	ld, err := svc.Leader("")
	assert.NoError(t, err)
	assert.Equal(t, "foo1", ld)
}

func TestService_Close(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc := tc.service()

	sc1, err := svc.Campaign("1", overrideOpts("foo1"))
	assert.NoError(t, err)
	assert.NoError(t, waitForStates(sc1, true, followerS, leaderS))

	sc2, err := svc.Campaign("2", nil)
	assert.NoError(t, err)
	assert.NoError(t, waitForStates(sc2, true, followerS, leaderS))

	assert.NoError(t, svc.Close())

	waitForStates(sc1, false, followerS)
	waitForStates(sc2, false, followerS)

	assert.NoError(t, svc.Close())
	assert.Error(t, svc.Resign(""))

	_, err = svc.Campaign("", nil)
	assert.Error(t, err)

	_, err = svc.Leader("")
	assert.Error(t, err)
}
