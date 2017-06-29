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

	sc, err := svc.Campaign("", 5, overrideOpts("foo1"))
	assert.NoError(t, err)
	assert.NoError(t, waitForStates(sc, true, followerS, leaderS))

	sc2, err := svc.Campaign("1", 5, overrideOpts("foo1"))
	assert.NoError(t, err)
	assert.NoError(t, waitForStates(sc2, true, followerS, leaderS))

	for _, eid := range []string{"", "1"} {
		ld, err := svc.Leader(eid, 5)
		assert.NoError(t, err)
		assert.Equal(t, "foo1", ld)
	}
}

func TestService_Resign(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc := tc.service()
	sc, err := svc.Campaign("e", 5, overrideOpts("foo1"))
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

	sc, err := svc.Campaign("", 5, overrideOpts("foo1"))
	assert.NoError(t, err)
	assert.NoError(t, waitForStates(sc, true, followerS, leaderS))

	_, err = svc.Leader("z", 5)
	assert.Equal(t, concurrency.ErrElectionNoLeader, err)

	ld, err := svc.Leader("", 5)
	assert.NoError(t, err)
	assert.Equal(t, "foo1", ld)
}

func TestService_Close(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc := tc.service()

	sc1, err := svc.Campaign("1", 5, overrideOpts("foo1"))
	assert.NoError(t, err)
	assert.NoError(t, waitForStates(sc1, true, followerS, leaderS))

	sc2, err := svc.Campaign("2", 5, nil)
	assert.NoError(t, err)
	assert.NoError(t, waitForStates(sc2, true, followerS, leaderS))

	assert.NoError(t, svc.Close())

	waitForStates(sc1, false, followerS)
	waitForStates(sc2, false, followerS)

	assert.NoError(t, svc.Close())
	assert.Error(t, svc.Resign(""))

	_, err = svc.Campaign("", 5, nil)
	assert.Error(t, err)

	_, err = svc.Leader("", 5)
	assert.Error(t, err)
}
