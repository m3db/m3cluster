package leader

import (
	"testing"

	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/stretchr/testify/assert"
)

func TestNewService(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc, err := NewService(tc.etcdClient(), tc.options(""))
	assert.NoError(t, err)
	assert.NotNil(t, svc)
}

func TestService_Campaign(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc := tc.service("foo1")

	wb, err := svc.Campaign("")
	assert.NoError(t, err)
	assert.NoError(t, waitForState(wb, CampaignLeader))

	_, err = svc.Campaign("")
	assert.Error(t, err)

	wb1, err := svc.Campaign("1")
	assert.NoError(t, err)
	assert.NoError(t, waitForState(wb1, CampaignLeader))

	for _, eid := range []string{"", "1"} {
		ld, err := svc.Leader(eid)
		assert.NoError(t, err)
		assert.Equal(t, "foo1", ld)
	}
}

func TestService_Resign(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc := tc.service("foo1")
	wb, err := svc.Campaign("e")
	assert.NoError(t, err)
	assert.NoError(t, waitForState(wb, CampaignLeader))

	err = svc.Resign("zzz")
	assert.Error(t, err)

	err = svc.Resign("e")
	assert.NoError(t, err)
	assert.NoError(t, waitForState(wb, CampaignFollower))
}

func TestService_Leader(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc := tc.service("foo1")

	wb, err := svc.Campaign("")
	assert.NoError(t, err)
	assert.NoError(t, waitForState(wb, CampaignLeader))

	_, err = svc.Leader("z")
	assert.Equal(t, concurrency.ErrElectionNoLeader, err)

	ld, err := svc.Leader("")
	assert.NoError(t, err)
	assert.Equal(t, "foo1", ld)
}

func TestService_Close(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc := tc.service("foo")

	wb1, err := svc.Campaign("1")
	assert.NoError(t, err)
	assert.NoError(t, waitForState(wb1, CampaignLeader))

	wb2, err := svc.Campaign("2")
	assert.NoError(t, err)
	assert.NoError(t, waitForState(wb2, CampaignLeader))

	assert.NoError(t, svc.Close())

	waitForState(wb1, CampaignClosed)
	waitForState(wb2, CampaignClosed)

	assert.NoError(t, svc.Close())
	assert.Error(t, svc.Resign(""))

	_, err = svc.Campaign("")
	assert.Error(t, err)

	_, err = svc.Leader("")
	assert.Error(t, err)
}
