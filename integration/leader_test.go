// +build integration

package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3cluster/internal/etcdcluster"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/leader"
	"github.com/m3db/m3x/watch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const defaultWait = 10 * time.Second

type conditionFn func() bool

func waitUntil(timeout time.Duration, fn conditionFn) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("fn not true within %s", timeout.String())
}

func waitForState(wb xwatch.Watch, target leader.CampaignState) error {
	return waitUntil(defaultWait, func() bool {
		if status, ok := wb.Get().(leader.CampaignStatus); ok {
			return ok && status.State == target
		}
		return false
	})
}

type testIntegrationCluster struct {
	*etcdcluster.Cluster

	t *testing.T
}

func newTestIntegrationCluster(t *testing.T, opts *etcdcluster.Options) *testIntegrationCluster {
	if opts == nil {
		opts = &etcdcluster.Options{}
	}

	cluster, err := etcdcluster.New(*opts)
	require.NoError(t, err)

	tc := &testIntegrationCluster{Cluster: cluster, t: t}

	tc.startAndWait(30 * time.Second)

	return tc
}

func (tc *testIntegrationCluster) service() services.LeaderService {
	opts := newTestOptions()
	client, err := tc.Client()
	require.NoError(tc.t, err)

	svc, err := leader.NewService(client, opts)
	require.NoError(tc.t, err)

	return svc
}

func (tc *testIntegrationCluster) startAndWait(timeout time.Duration) {
	err := tc.Start()
	require.NoError(tc.t, err)

	err = tc.WaitReady(timeout)
	require.NoError(tc.t, err)
}

func campaign(svc services.LeaderService, eid string, ttl int, val string) (xwatch.Watch, error) {
	return svc.Campaign(eid, ttl, services.NewCampaignOptions().SetLeaderValue(val))
}

func newTestOptions() leader.Options {
	sid := services.NewServiceID().
		SetEnvironment("e1").
		SetZone("z1").
		SetName("s1")

	eo := services.NewElectionOptions().
		SetLeaderTimeout(10 * time.Second).
		SetResignTimeout(10 * time.Second)

	return leader.NewOptions().
		SetServiceID(sid).
		SetElectionOpts(eo)
}

func TestIntegration_Simple(t *testing.T) {
	tc := newTestIntegrationCluster(t, nil)
	defer tc.Shutdown()

	svc1 := tc.service()
	svc2 := tc.service()

	wb1, err := campaign(svc1, "", 5, "i1")
	assert.NoError(t, err)
	assert.NoError(t, waitForState(wb1, leader.CampaignLeader))

	wb2, err := campaign(svc2, "", 5, "i2")
	assert.NoError(t, err)
	assert.NoError(t, waitForState(wb2, leader.CampaignFollower))

	ld, err := svc1.Leader("", 5)
	assert.NoError(t, err)
	assert.Equal(t, "i1", ld)

	err = svc1.Resign("")
	assert.NoError(t, err)
	assert.NoError(t, waitForState(wb1, leader.CampaignFollower))
	assert.NoError(t, waitForState(wb2, leader.CampaignLeader))

	ld1, err := svc1.Leader("", 5)
	assert.NoError(t, err)
	assert.Equal(t, "i2", ld1)

	ld2, err := svc2.Leader("", 5)
	assert.NoError(t, err)
	assert.Equal(t, "i2", ld2)
}

func TestIntegration_Leader_KillNode(t *testing.T) {
	tc := newTestIntegrationCluster(t, nil)
	defer tc.Shutdown()

	svc1 := tc.service()
	svc2 := tc.service()

	wb1, err := campaign(svc1, "", 5, "i1")
	assert.NoError(t, err)
	assert.NoError(t, waitForState(wb1, leader.CampaignLeader))

	wb2, err := campaign(svc2, "", 5, "i2")
	assert.NoError(t, err)
	assert.NoError(t, waitForState(wb2, leader.CampaignFollower))

	ld, err := svc1.Leader("", 5)
	assert.NoError(t, err)
	assert.Equal(t, "i1", ld)

	err = tc.KillLeader(false)
	assert.NoError(t, err)

	ld, err = svc1.Leader("", 5)
	assert.NoError(t, err)
	assert.Equal(t, "i1", ld)

	err = svc1.Resign("")
	assert.NoError(t, err)
	assert.NoError(t, waitForState(wb1, leader.CampaignFollower))
	assert.NoError(t, waitForState(wb2, leader.CampaignLeader))

	ld1, err := svc1.Leader("", 5)
	assert.NoError(t, err)
	assert.Equal(t, "i2", ld1)

	ld2, err := svc2.Leader("", 5)
	assert.NoError(t, err)
	assert.Equal(t, "i2", ld2)
}
