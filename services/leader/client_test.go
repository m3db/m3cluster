package leader

import (
	"testing"
	"time"

	"fmt"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/coreos/etcd/integration"
	"github.com/m3db/m3cluster/services"
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

func waitForState(timeout time.Duration, wb xwatch.Watch, target CampaignState) error {
	return waitUntil(timeout, func() bool {
		if state, ok := wb.Get().(CampaignState); ok {
			return ok && state == target
		}
		return false
	})
}

type testCluster struct {
	t       *testing.T
	cluster *integration.ClusterV3
}

func newTestCluster(t *testing.T) *testCluster {
	return &testCluster{
		t: t,
		cluster: integration.NewClusterV3(t, &integration.ClusterConfig{
			Size: 1,
		}),
	}
}

func (tc *testCluster) close() {
	tc.cluster.Terminate(tc.t)
}

func (tc *testCluster) etcdClient() *clientv3.Client {
	return tc.cluster.RandClient()
}

func (tc *testCluster) options(override string) Options {
	sid := services.NewServiceID().
		SetEnvironment("e1").
		SetName("s1").
		SetZone("z1")

	return NewOptions().
		SetOverrideValue(override).
		SetServiceID(sid)
}

func (tc *testCluster) service(override string) *client {
	svc, err := newClient(tc.etcdClient(), tc.options(override))
	require.NoError(tc.t, err)

	return svc
}

func TestNewClient(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc, err := newClient(tc.etcdClient(), tc.options(""))
	assert.NoError(t, err)
	assert.NotNil(t, svc)
}

func TestCampaign(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc := tc.service("")

	wb, err := svc.Campaign()
	assert.NoError(t, err)

	assert.NoError(t, waitForState(defaultWait, wb, CampaignLeader))

	ld, err := svc.Leader()
	assert.NoError(t, err)
	assert.Equal(t, svc.val, ld)
}

func TestResign(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc := tc.service("i1")

	wb, err := svc.Campaign()
	assert.NoError(t, err)

	assert.NoError(t, waitForState(defaultWait, wb, CampaignLeader))

	ld, err := svc.Leader()
	assert.NoError(t, err)
	assert.Equal(t, "i1", ld)

	err = svc.Resign()
	assert.NoError(t, err)

	assert.NoError(t, waitForState(defaultWait, wb, CampaignFollower))

	ld, err = svc.Leader()
	assert.Equal(t, concurrency.ErrElectionNoLeader, err)
	assert.Equal(t, "", ld)
}

func testHandoff(t *testing.T, resign bool) {
	tc := newTestCluster(t)
	defer tc.close()

	svc1, svc2 := tc.service("i1"), tc.service("i2")

	wb1, err := svc1.Campaign()
	assert.NoError(t, err)
	assert.NoError(t, waitForState(defaultWait, wb1, CampaignLeader))

	wb2, err := svc2.Campaign()
	assert.NoError(t, waitForState(defaultWait, wb2, CampaignFollower))

	ld, err := svc1.Leader()
	assert.NoError(t, err)
	assert.Equal(t, ld, "i1")

	if resign {
		err = svc1.Resign()
		assert.NoError(t, waitForState(defaultWait, wb1, CampaignFollower))
	} else {
		err = svc1.Close()
		assert.NoError(t, waitForState(defaultWait, wb1, CampaignClosed))
	}
	assert.NoError(t, err)

	assert.NoError(t, waitForState(defaultWait, wb2, CampaignLeader))

	ld, err = svc2.Leader()
	assert.NoError(t, err)
	assert.Equal(t, ld, "i2")
}

func TestCampaign_Cancel_Resign(t *testing.T) {
	testHandoff(t, true)
}

func TestCampaign_Cancel_Close(t *testing.T) {
	testHandoff(t, false)
}

func TestCampaign_Close_NonLeader(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc1, svc2 := tc.service("i1"), tc.service("i2")

	wb1, err := svc1.Campaign()
	assert.NoError(t, err)
	assert.NoError(t, waitForState(defaultWait, wb1, CampaignLeader))

	wb2, err := svc2.Campaign()
	assert.NoError(t, waitForState(defaultWait, wb2, CampaignFollower))

	ld, err := svc1.Leader()
	assert.NoError(t, err)
	assert.Equal(t, ld, "i1")

	err = svc2.Close()
	assert.NoError(t, err)
	assert.NoError(t, waitForState(defaultWait, wb2, CampaignClosed))

	err = svc1.Resign()
	assert.NoError(t, waitForState(defaultWait, wb1, CampaignFollower))

	ld, err = svc2.Leader()
	assert.Equal(t, concurrency.ErrElectionNoLeader, err)
}

func TestClose(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc := tc.service("i1")

	wb, err := svc.Campaign()
	assert.NoError(t, err)
	assert.NoError(t, waitForState(defaultWait, wb, CampaignLeader))

	ld, err := svc.Leader()
	assert.NoError(t, err)
	assert.Equal(t, "i1", ld)

	err = svc.Close()
	assert.NoError(t, err)
	assert.True(t, svc.isClosed())
	assert.NoError(t, waitForState(defaultWait, wb, CampaignClosed))

	err = svc.Resign()
	assert.Equal(t, ErrClientClosed, err)

	_, err = svc.Campaign()
	assert.Equal(t, ErrClientClosed, err)
}

func TestLeader(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc1, svc2 := tc.service("i1"), tc.service("i2")
	wb, err := svc1.Campaign()
	assert.NoError(t, err)
	assert.NoError(t, waitForState(defaultWait, wb, CampaignLeader))

	ld, err := svc2.Leader()
	assert.NoError(t, err)

	assert.Equal(t, "i1", ld)
}

func TestElectionPrefix(t *testing.T) {
	for args, exp := range map[*struct {
		env, name, eid string
	}]string{
		{"", "svc", ""}:       "_ld/svc/SVC_WIDE_ELECTION",
		{"env", "svc", ""}:    "_ld/env/svc/SVC_WIDE_ELECTION",
		{"", "svc", "foo"}:    "_ld/svc/foo",
		{"env", "svc", "foo"}: "_ld/env/svc/foo",
	} {
		sid := services.NewServiceID().
			SetEnvironment(args.env).
			SetName(args.name)

		eo := services.NewElectionOptions().
			SetElectionID(args.eid)

		opts := NewOptions().
			SetServiceID(sid).
			SetElectionOpts(eo)

		pfx := electionPrefix(opts)

		assert.Equal(t, exp, pfx)
	}
}
