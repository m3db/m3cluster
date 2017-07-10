package leader

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/leader/campaign"
	"github.com/m3db/m3cluster/services/leader/election"

	"golang.org/x/net/context"
)

// appended to elections with an empty string for electionID to make it easier
// for user to debug etcd keys
const defaultElectionID = "default"

var (
	// ErrNoLeader is returned when a call to Leader() is made to an election
	// with no leader. We duplicate this error so the user doesn't have to
	// import etcd's concurrency package in order to check the cause of the
	// error.
	ErrNoLeader = concurrency.ErrElectionNoLeader
)

// NB(mschalle): when an etcd leader failover occurs, all current leases have
// their TTLs refreshed: https://github.com/coreos/etcd/issues/2660

type client struct {
	sync.RWMutex

	ecl       *election.Client
	opts      services.ElectionOptions
	ctxCancel context.CancelFunc
	resignC   chan struct{}
	closed    uint32
}

// newClient returns an instance of an client bound to a single election.
func newClient(cli *clientv3.Client, opts Options, electionID string, ttl int) (*client, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	var sessionOpts []concurrency.SessionOption
	if ttl != 0 {
		sessionOpts = append(sessionOpts, concurrency.WithTTL(ttl))
	}

	pfx := electionPrefix(opts.ServiceID(), electionID)
	ec, err := election.NewClient(cli, pfx, election.WithSessionOptions(sessionOpts...))
	if err != nil {
		return nil, err
	}

	return &client{
		ecl:     ec,
		opts:    opts.ElectionOpts(),
		resignC: make(chan struct{}),
	}, nil
}

func (c *client) campaign(opts services.CampaignOptions) (<-chan campaign.Status, error) {
	if c.isClosed() {
		return nil, errClientClosed
	}

	ctx, cancel := context.WithCancel(context.Background())
	c.Lock()
	c.ctxCancel = cancel
	c.Unlock()

	proposeVal := c.val(opts.LeaderValue())

	// buffer 1 to not block initial follower update
	sc := make(chan campaign.Status, 1)

	sc <- campaign.NewStatus(campaign.Follower)

	go func() {
		defer close(sc)
		defer cancel()

		// Campaign blocks until elected. Once we are elected, we get a channel
		// that's closed if our session dies.
		ch, err := c.ecl.Campaign(ctx, proposeVal)
		if err != nil {
			sc <- campaign.NewErrCampaignStatus(err)
			return
		}

		sc <- campaign.NewStatus(campaign.Leader)
		select {
		case <-ch:
			sc <- campaign.NewErrCampaignStatus(election.ErrSessionExpired)
		case <-c.resignC:
			sc <- campaign.NewStatus(campaign.Follower)
		}
	}()

	return sc, nil
}

func (c *client) resign() error {
	if c.isClosed() {
		return errClientClosed
	}

	// if there's an active blocking call to Campaign() stop it
	c.Lock()
	if c.ctxCancel != nil {
		c.ctxCancel()
		c.ctxCancel = nil
	}
	c.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), c.opts.ResignTimeout())
	defer cancel()
	if err := c.ecl.Resign(ctx); err != nil {
		return err
	}

	// if successfully resigned and there was a campaign in Leader state cancel
	// it
	select {
	case c.resignC <- struct{}{}:
	default:
	}

	return nil
}

func (c *client) leader() (string, error) {
	if c.isClosed() {
		return "", errClientClosed
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.opts.LeaderTimeout())
	ld, err := c.ecl.Leader(ctx)
	cancel()
	if err == concurrency.ErrElectionNoLeader {
		return ld, ErrNoLeader
	}
	return ld, err
}

// Close closes the election service client entirely. No more campaigns can be
// started and any outstanding campaigns are closed.
func (c *client) close() error {
	atomic.StoreUint32(&c.closed, 1)
	return c.ecl.Close()
}

func (c *client) isClosed() bool {
	return atomic.LoadUint32(&c.closed) == 1
}

// val returns the value the leader should propose based on (1) a potentially
// empty override value and (2) the hostname of the caller (with a fallback to
// the DefaultValue option).
func (c *client) val(override string) string {
	if override != "" {
		return override
	}

	return c.opts.Hostname()
}

// elections for a service "svc" in env "test" should be stored under
// "_ld/test/svc". A service "svc" with no environment will be stored under
// "_ld/svc".
func servicePrefix(sid services.ServiceID) string {
	env := sid.Environment()
	if env == "" {
		return fmt.Sprintf(keyFormat, leaderKeyPrefix, sid.Name())
	}

	return fmt.Sprintf(
		keyFormat,
		leaderKeyPrefix,
		fmt.Sprintf(keyFormat, env, sid.Name()))
}

func electionPrefix(sid services.ServiceID, electionID string) string {
	eid := electionID
	if eid == "" {
		eid = defaultElectionID
	}

	return fmt.Sprintf(
		keyFormat,
		servicePrefix(sid),
		eid)
}
