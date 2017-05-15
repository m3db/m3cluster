package leader

import (
	"testing"
	"time"

	"github.com/m3db/m3cluster/internal/etcdcluster"
	"github.com/m3db/m3cluster/services"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func (tc *testIntegrationCluster) service(override string, ttl int) services.LeaderService {
	opts := newTestOptions(override, ttl)
	client, err := tc.Client()
	require.NoError(tc.t, err)

	svc, err := NewService(client, opts)
	require.NoError(tc.t, err)

	return svc
}

func (tc *testIntegrationCluster) startAndWait(timeout time.Duration) {
	err := tc.Start()
	require.NoError(tc.t, err)

	err = tc.WaitReady(timeout)
	require.NoError(tc.t, err)
}

func newTestOptions(override string, ttl int) Options {
	sid := services.NewServiceID().
		SetEnvironment("e1").
		SetZone("z1").
		SetName("s1")

	return NewOptions().
		SetServiceID(sid).
		SetOverrideValue(override).
		SetTTL(ttl)
}

func TestIntegration_Simple(t *testing.T) {
	tc := newTestIntegrationCluster(t, nil)
	defer tc.Shutdown()

	ld2ch := make(chan struct{})

	svc1 := tc.service("i1", 5)
	svc2 := tc.service("i2", 5)

	err := svc1.Campaign()
	assert.NoError(t, err)

	go func() {
		err := svc2.Campaign()
		ld2ch <- struct{}{}
		assert.NoError(t, err)
	}()

	ld, err := svc1.Leader()
	assert.NoError(t, err)
	assert.Equal(t, "i1", ld)

	err = svc1.Resign()
	assert.NoError(t, err)

	<-ld2ch

	ld1, err := svc1.Leader()
	assert.NoError(t, err)
	assert.Equal(t, "i2", ld1)

	ld2, err := svc2.Leader()
	assert.NoError(t, err)
	assert.Equal(t, "i2", ld2)
}

func TestIntegration_Leader_KillNode(t *testing.T) {
	tc := newTestIntegrationCluster(t, nil)
	defer tc.Shutdown()

	ld2ch := make(chan struct{})

	svc1 := tc.service("i1", 5)
	svc2 := tc.service("i2", 5)

	err := svc1.Campaign()
	assert.NoError(t, err)

	go func() {
		err := svc2.Campaign()
		ld2ch <- struct{}{}
		assert.NoError(t, err)
	}()

	ld, err := svc1.Leader()
	assert.NoError(t, err)
	assert.Equal(t, "i1", ld)

	err = tc.KillLeader(false)
	assert.NoError(t, err)

	ld, err = svc1.Leader()
	assert.NoError(t, err)
	assert.Equal(t, "i1", ld)

	err = svc1.Resign()
	assert.NoError(t, err)

	<-ld2ch

	ld1, err := svc1.Leader()
	assert.NoError(t, err)
	assert.Equal(t, "i2", ld1)

	ld2, err := svc2.Leader()
	assert.NoError(t, err)
	assert.Equal(t, "i2", ld2)
}
