package leader

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
		if status, ok := wb.Get().(CampaignStatus); ok {
			return ok && status.State == target
		}
		return false
	})
}

type testCluster struct {
	t       *testing.T
	cluster *integration.ClusterV3
	opts    services.CampaignOptions
}

func newTestCluster(t *testing.T) *testCluster {
	return &testCluster{
		t: t,
		cluster: integration.NewClusterV3(t, &integration.ClusterConfig{
			Size: 1,
		}),
		opts: services.NewCampaignOptions(),
	}
}

func (tc *testCluster) close() {
	tc.cluster.Terminate(tc.t)
}

func (tc *testCluster) etcdClient() *clientv3.Client {
	return tc.cluster.RandClient()
}

func (tc *testCluster) options() Options {
	sid := services.NewServiceID().
		SetEnvironment("e1").
		SetName("s1").
		SetZone("z1")

	return NewOptions().
		SetServiceID(sid)
}

func (tc *testCluster) client() *client {
	svc, err := newClient(tc.etcdClient(), tc.options(), "", 5)
	require.NoError(tc.t, err)

	return svc
}

func (tc *testCluster) service() services.LeaderService {
	svc, err := NewService(tc.etcdClient(), tc.options())
	require.NoError(tc.t, err)

	return svc
}

func TestNewClient(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc, err := newClient(tc.etcdClient(), tc.options(), "", 5)
	assert.NoError(t, err)
	assert.NotNil(t, svc)
}

func TestNewClient_BadCluster(t *testing.T) {
	tc := newTestCluster(t)
	cl := tc.etcdClient()
	tc.close()

	_, err := newClient(cl, tc.options(), "", 5)
	assert.Error(t, err)
}

func TestCampaign(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc := tc.client()

	wb, err := svc.campaign("", tc.opts)
	assert.NoError(t, err)
	assert.NoError(t, waitForState(wb, CampaignLeader))

	ld, err := svc.leader()
	assert.NoError(t, err)
	assert.Equal(t, svc.val(""), ld)

	assert.Equal(t, uint32(1), atomic.LoadUint32(&svc.campaigning))
	wb, err = svc.campaign("", tc.opts)
	assert.NoError(t, err)
	assert.NoError(t, waitForState(wb, CampaignLeader))

	_, err = svc.campaign("foo", tc.opts)
	assert.Error(t, err)
	t.Log(err)
}

func TestCampaign_Override(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc := tc.client()

	wb, err := svc.campaign("foo", tc.opts)
	assert.NoError(t, err)
	assert.NoError(t, waitForState(wb, CampaignLeader))

	ld, err := svc.leader()
	assert.NoError(t, err)
	assert.Equal(t, "foo", ld)
}

func TestCampaign_Renew(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc := tc.client()
	wb, err := svc.campaign("", tc.opts)
	assert.NoError(t, err)
	assert.NoError(t, waitForState(wb, CampaignLeader))

	err = svc.resign()
	assert.NoError(t, err)
	assert.NoError(t, waitForState(wb, CampaignFollower))

	_, err = svc.leader()
	assert.Equal(t, ErrNoLeader, err)

	wb2, err := svc.campaign("", tc.opts)
	assert.NoError(t, err)
	assert.NoError(t, waitForState(wb2, CampaignLeader))
}

func TestCampaign_Failure(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc := tc.client()
	wb, err := svc.campaign("i1", tc.opts)
	assert.NoError(t, err)
	assert.NoError(t, waitForState(wb, CampaignLeader))

	svc2 := tc.client()
	wb2, err := svc2.campaign("i2", tc.opts)
	assert.NoError(t, err)

	var mu sync.Mutex
	statuses := []CampaignStatus{}

	go func() {
		for range wb2.C() {
			mu.Lock()
			statuses = append(statuses, wb2.Get().(CampaignStatus))
			mu.Unlock()
		}
	}()

	// session expiration should cause it to retry campaign with new session
	svc2.session.Close()

	// should see follower state, then error (while getting new session), then
	// follower again
	waitUntil(15*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		if len(statuses) < 3 {
			return false
		}

		var followerFirst, sawError, followerLast bool

		followerFirst = statuses[0].State == CampaignFollower
		followerLast = statuses[len(statuses)-1].State == CampaignFollower

		for i := 1; i < len(statuses)-1; i++ {
			if statuses[i].State == CampaignError {
				sawError = true
			}
		}

		return followerFirst && sawError && followerLast
	})
}

func TestResign(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc := tc.client()

	wb, err := svc.campaign("i1", tc.opts)
	assert.NoError(t, err)

	assert.NoError(t, waitForState(wb, CampaignLeader))

	ld, err := svc.leader()
	assert.NoError(t, err)
	assert.Equal(t, "i1", ld)

	err = svc.resign()
	assert.NoError(t, err)

	assert.NoError(t, waitForState(wb, CampaignFollower))

	ld, err = svc.leader()
	assert.Equal(t, concurrency.ErrElectionNoLeader, err)
	assert.Equal(t, "", ld)
}

func TestResign_Early(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc := tc.client()

	err := svc.resign()
	assert.NoError(t, err)
}

func testHandoff(t *testing.T, resign bool) {
	tc := newTestCluster(t)
	defer tc.close()

	svc1, svc2 := tc.client(), tc.client()

	wb1, err := svc1.campaign("i1", tc.opts)
	assert.NoError(t, err)
	assert.NoError(t, waitForState(wb1, CampaignLeader))

	wb2, err := svc2.campaign("i2", tc.opts)
	assert.NoError(t, waitForState(wb2, CampaignFollower))

	ld, err := svc1.leader()
	assert.NoError(t, err)
	assert.Equal(t, ld, "i1")

	if resign {
		err = svc1.resign()
		assert.NoError(t, waitForState(wb1, CampaignFollower))
	} else {
		err = svc1.close()
		assert.NoError(t, waitForState(wb1, CampaignClosed))
	}
	assert.NoError(t, err)

	assert.NoError(t, waitForState(wb2, CampaignLeader))

	ld, err = svc2.leader()
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

	svc1, svc2 := tc.client(), tc.client()

	wb1, err := svc1.campaign("i1", tc.opts)
	assert.NoError(t, err)
	assert.NoError(t, waitForState(wb1, CampaignLeader))

	wb2, err := svc2.campaign("i2", tc.opts)
	assert.NoError(t, waitForState(wb2, CampaignFollower))

	ld, err := svc1.leader()
	assert.NoError(t, err)
	assert.Equal(t, ld, "i1")

	err = svc2.close()
	assert.NoError(t, err)
	assert.NoError(t, waitForState(wb2, CampaignClosed))

	err = svc1.resign()
	assert.NoError(t, waitForState(wb1, CampaignFollower))

	ld, err = svc2.leader()
	assert.Equal(t, concurrency.ErrElectionNoLeader, err)
}

func TestClose(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc := tc.client()

	wb, err := svc.campaign("i1", tc.opts)
	assert.NoError(t, err)
	assert.NoError(t, waitForState(wb, CampaignLeader))

	ld, err := svc.leader()
	assert.NoError(t, err)
	assert.Equal(t, "i1", ld)

	err = svc.close()
	assert.NoError(t, err)
	assert.True(t, svc.isClosed())
	assert.NoError(t, waitForState(wb, CampaignClosed))

	err = svc.resign()
	assert.Equal(t, errClientClosed, err)

	_, err = svc.campaign("", tc.opts)
	assert.Equal(t, errClientClosed, err)
}

func TestLeader(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc1, svc2 := tc.client(), tc.client()
	wb, err := svc1.campaign("i1", tc.opts)
	assert.NoError(t, err)
	assert.NoError(t, waitForState(wb, CampaignLeader))

	ld, err := svc2.leader()
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
