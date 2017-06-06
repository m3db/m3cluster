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

func waitForState(wb xwatch.Watch, target CampaignState) error {
	return waitUntil(defaultWait, func() bool {
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

func (tc *testCluster) client(override string) *client {
	svc, err := newClient(tc.etcdClient(), tc.options(override), "", 5)
	require.NoError(tc.t, err)

	return svc
}

func (tc *testCluster) service(override string) services.LeaderService {
	svc, err := NewService(tc.etcdClient(), tc.options(override))
	require.NoError(tc.t, err)

	return svc
}

func TestNewClient(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc, err := newClient(tc.etcdClient(), tc.options(""), "", 5)
	assert.NoError(t, err)
	assert.NotNil(t, svc)
}

func TestCampaign(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc := tc.client("")

	wb, err := svc.Campaign()
	assert.NoError(t, err)

	assert.NoError(t, waitForState(wb, CampaignLeader))

	ld, err := svc.Leader()
	assert.NoError(t, err)
	assert.Equal(t, svc.val, ld)

	_, err = svc.Campaign()
	assert.Equal(t, ErrCampaignInProgress, err)
}

func TestResign(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc := tc.client("i1")

	wb, err := svc.Campaign()
	assert.NoError(t, err)

	assert.NoError(t, waitForState(wb, CampaignLeader))

	ld, err := svc.Leader()
	assert.NoError(t, err)
	assert.Equal(t, "i1", ld)

	err = svc.Resign()
	assert.NoError(t, err)

	assert.NoError(t, waitForState(wb, CampaignFollower))

	ld, err = svc.Leader()
	assert.Equal(t, concurrency.ErrElectionNoLeader, err)
	assert.Equal(t, "", ld)
}

func TestResign_Early(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc := tc.client("i1")

	err := svc.Resign()
	assert.NoError(t, err)
}

func testHandoff(t *testing.T, resign bool) {
	tc := newTestCluster(t)
	defer tc.close()

	svc1, svc2 := tc.client("i1"), tc.client("i2")

	wb1, err := svc1.Campaign()
	assert.NoError(t, err)
	assert.NoError(t, waitForState(wb1, CampaignLeader))

	wb2, err := svc2.Campaign()
	assert.NoError(t, waitForState(wb2, CampaignFollower))

	ld, err := svc1.Leader()
	assert.NoError(t, err)
	assert.Equal(t, ld, "i1")

	if resign {
		err = svc1.Resign()
		assert.NoError(t, waitForState(wb1, CampaignFollower))
	} else {
		err = svc1.Close()
		assert.NoError(t, waitForState(wb1, CampaignClosed))
	}
	assert.NoError(t, err)

	assert.NoError(t, waitForState(wb2, CampaignLeader))

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

	svc1, svc2 := tc.client("i1"), tc.client("i2")

	wb1, err := svc1.Campaign()
	assert.NoError(t, err)
	assert.NoError(t, waitForState(wb1, CampaignLeader))

	wb2, err := svc2.Campaign()
	assert.NoError(t, waitForState(wb2, CampaignFollower))

	ld, err := svc1.Leader()
	assert.NoError(t, err)
	assert.Equal(t, ld, "i1")

	err = svc2.Close()
	assert.NoError(t, err)
	assert.NoError(t, waitForState(wb2, CampaignClosed))

	err = svc1.Resign()
	assert.NoError(t, waitForState(wb1, CampaignFollower))

	ld, err = svc2.Leader()
	assert.Equal(t, concurrency.ErrElectionNoLeader, err)
}

func TestClose(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc := tc.client("i1")

	wb, err := svc.Campaign()
	assert.NoError(t, err)
	assert.NoError(t, waitForState(wb, CampaignLeader))

	ld, err := svc.Leader()
	assert.NoError(t, err)
	assert.Equal(t, "i1", ld)

	err = svc.Close()
	assert.NoError(t, err)
	assert.True(t, svc.isClosed())
	assert.NoError(t, waitForState(wb, CampaignClosed))

	err = svc.Resign()
	assert.Equal(t, ErrClientClosed, err)

	_, err = svc.Campaign()
	assert.Equal(t, ErrClientClosed, err)
}

func TestLeader(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc1, svc2 := tc.client("i1"), tc.client("i2")
	wb, err := svc1.Campaign()
	assert.NoError(t, err)
	assert.NoError(t, waitForState(wb, CampaignLeader))

	ld, err := svc2.Leader()
	assert.NoError(t, err)

	assert.Equal(t, "i1", ld)
}

func TestElectionPrefix(t *testing.T) {
	for args, exp := range map[*struct {
		env, name, eid string
	}]string{
		{"", "svc", ""}:       "_ld/svc/default",
		{"env", "svc", ""}:    "_ld/env/svc/default",
		{"", "svc", "foo"}:    "_ld/svc/foo",
		{"env", "svc", "foo"}: "_ld/env/svc/foo",
	} {
		sid := services.NewServiceID().
			SetEnvironment(args.env).
			SetName(args.name)

		pfx := electionPrefix(sid, args.eid)

		assert.Equal(t, exp, pfx)
	}
}
