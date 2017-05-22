package leader

import (
	"testing"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/coreos/etcd/integration"
	"github.com/m3db/m3cluster/services"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testCluster struct {
	t       *testing.T
	cluster *integration.ClusterV3
}

func (tc *testCluster) close() {
	tc.cluster.Terminate(tc.t)
}

func (tc *testCluster) client() *clientv3.Client {
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

func (tc *testCluster) service(override string) services.LeaderService {
	svc, err := NewService(tc.client(), tc.options(override))
	require.NoError(tc.t, err)

	return svc
}

func TestNewService(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc, err := NewService(tc.client(), tc.options(""))
	assert.NoError(t, err)
	assert.NotNil(t, svc)
}

func TestCampaign(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc := tc.service("")

	err := svc.Campaign()
	assert.NoError(t, err)

	ld, err := svc.Leader()
	assert.NoError(t, err)
	assert.Equal(t, svc.(*client).val, ld)
}

func TestResign(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc := tc.service("i1")

	err := svc.Campaign()
	assert.NoError(t, err)

	ld, err := svc.Leader()
	assert.NoError(t, err)
	assert.Equal(t, "i1", ld)

	err = svc.Resign()
	assert.NoError(t, err)

	ld, err = svc.Leader()
	assert.Equal(t, concurrency.ErrElectionNoLeader, err)
	assert.Equal(t, "", ld)
}

func testHandoff(t *testing.T, resign bool) {
	tc := newTestCluster(t)
	defer tc.close()

	// need this channel before in order to make sure we don't close the test
	// cluster before the svc2 campaign goroutine has an opportunity to complete
	ld2ch := make(chan struct{})

	svc1, svc2 := tc.service("i1"), tc.service("i2")

	err := svc1.Campaign()
	assert.NoError(t, err)

	go func() {
		err := svc2.Campaign()
		ld2ch <- struct{}{}
		assert.NoError(t, err)
	}()

	ld, err := svc1.Leader()
	assert.NoError(t, err)
	assert.Equal(t, ld, "i1")

	if resign {
		err = svc1.Resign()
	} else {
		err = svc1.Close()
	}
	assert.NoError(t, err)

	<-ld2ch

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

	// need this channel before in order to make sure we don't close the test
	// cluster before the svc2 campaign goroutine has an opportunity to complete
	ld2ch := make(chan struct{})

	svc1, svc2 := tc.service("i1"), tc.service("i2")

	err := svc1.Campaign()
	assert.NoError(t, err)

	go func() {
		err := svc2.Campaign()
		ld2ch <- struct{}{}
		assert.Error(t, err)
	}()

	ld, err := svc1.Leader()
	assert.NoError(t, err)
	assert.Equal(t, ld, "i1")

	err = svc2.Close()
	assert.NoError(t, err)

	err = svc1.Resign()
	assert.NoError(t, err)

	<-ld2ch

	ld, err = svc2.Leader()
	assert.Equal(t, concurrency.ErrElectionNoLeader, err)
}

func TestClose(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc := tc.service("i1")

	err := svc.Campaign()
	assert.NoError(t, err)

	ld, err := svc.Leader()
	assert.NoError(t, err)
	assert.Equal(t, "i1", ld)

	err = svc.Close()
	assert.NoError(t, err)
	assert.True(t, svc.(*client).isClosed())

	err = svc.Resign()
	assert.Equal(t, errClientClosed, err)

	err = svc.Campaign()
	assert.Equal(t, errClientClosed, err)
}

func TestLeader(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc1, svc2 := tc.service("i1"), tc.service("i2")
	err := svc1.Campaign()
	assert.NoError(t, err)

	ld, err := svc2.Leader()
	assert.NoError(t, err)

	assert.Equal(t, "i1", ld)
}

func newTestCluster(t *testing.T) *testCluster {
	return &testCluster{
		t: t,
		cluster: integration.NewClusterV3(t, &integration.ClusterConfig{
			Size: 1,
		}),
	}
}
