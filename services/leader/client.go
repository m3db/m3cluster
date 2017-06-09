package leader

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3x/watch"

	"golang.org/x/net/context"
)

// appended to elections with an empty string for electionID to make it easier
// for user to debug etcd keys
const defaultElectionSuffix = "default"

var (
	// ErrNoLeader is returned when a call to Leader() is made to an election
	// with no leader. We duplicate this error so the user doesn't have to
	// import etcd's concurrency package in order to check the cause of the
	// error.
	ErrNoLeader = concurrency.ErrElectionNoLeader

	// ErrSessionExpired is returned when a client's session (etcd lease) is no
	// longer being refreshed for any reason (due to expiration, error state,
	// etc.).
	ErrSessionExpired = errors.New("election client session (lease) expired")
)

// NB(mschalle): when an etcd leader failover occurs, all current leases have
// their TTLs refreshed: https://github.com/coreos/etcd/issues/2660

type client struct {
	sync.RWMutex

	ctxCancel   context.CancelFunc
	election    *concurrency.Election
	session     *concurrency.Session
	opts        services.ElectionOptions
	closed      uint32
	campaigning uint32
	wb          xwatch.Watchable
	campaignVal string
}

// newClient returns an instance of an client client bound to a single election.
func newClient(cli *clientv3.Client, opts Options, electionID string, ttl int) (*client, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	var sessionOpts []concurrency.SessionOption
	if ttl != 0 {
		sessionOpts = append(sessionOpts, concurrency.WithTTL(ttl))
	}

	session, err := concurrency.NewSession(cli, sessionOpts...)
	if err != nil {
		return nil, err
	}

	election := concurrency.NewElection(session, electionPrefix(opts.ServiceID(), electionID))

	wb := xwatch.NewWatchable()
	wb.Update(newCampaignStatus(CampaignFollower))

	return &client{
		election: election,
		session:  session,
		opts:     opts.ElectionOpts(),
		wb:       wb,
	}, nil
}

func (c *client) campaign(override string) (xwatch.Watch, error) {
	if c.isClosed() {
		return nil, errClientClosed
	}

	proposeVal := c.val(override)

	if !atomic.CompareAndSwapUint32(&c.campaigning, 0, 1) {
		c.RLock()
		lastVal := c.campaignVal
		c.RUnlock()

		if lastVal != proposeVal {
			return nil, fmt.Errorf("cannot start simultaneous campaign with value '%s', one in progress with '%s'", proposeVal, lastVal)
		}
		return c.newWatch()
	}

	ctx, cancel := context.WithCancel(context.Background())

	c.Lock()
	c.ctxCancel = cancel
	c.Unlock()

	go func() {

		c.Lock()
		c.campaignVal = proposeVal
		c.Unlock()

		c.wb.Update(newCampaignStatus(CampaignFollower))
		// blocks until elected or error
		err := c.election.Campaign(ctx, proposeVal)
		if err != nil {
			c.wb.Update(newErrCampaignStatus(err))
		}

		failed := false
		select {
		case <-c.session.Done():
			failed = true
			c.wb.Update(newErrCampaignStatus(ErrSessionExpired))
		default:
			c.wb.Update(newCampaignStatus(CampaignLeader))
		}

		cancel()
		c.Lock()
		c.ctxCancel = nil
		if failed {
			c.resetCampaignWithLock()
		}
		c.Unlock()
	}()

	return c.newWatch()
}

func (c *client) resign() error {
	if c.isClosed() {
		return errClientClosed
	}

	c.Lock()
	defer c.Unlock()

	// if we're not the leader but still campaigning, cancelling the context
	// will stop the campaign
	c.cancelWithLock()

	ctx, cancel := context.WithTimeout(context.Background(), c.opts.ResignTimeout())
	defer cancel()
	if err := c.election.Resign(ctx); err != nil {
		c.wb.Update(newErrCampaignStatus(err))
		return err
	}

	c.resetCampaignWithLock()
	c.wb.Update(newCampaignStatus(CampaignFollower))
	return nil
}

func (c *client) leader() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.opts.LeaderTimeout())
	ld, err := c.election.Leader(ctx)
	cancel()
	if err == concurrency.ErrElectionNoLeader {
		return ld, ErrNoLeader
	}
	return ld, err
}

func (c *client) resetCampaignWithLock() {
	atomic.StoreUint32(&c.campaigning, 0)
	c.campaignVal = ""
}

// Close closes the election service client entirely. No more campaigns can be
// started and any outstanding campaigns are closed.
func (c *client) close() error {
	if c.setClosed() {
		c.Lock()
		c.cancelWithLock()
		c.Unlock()

		c.wb.Update(newCampaignStatus(CampaignClosed))
		c.wb.Close()
		return c.session.Close()
	}

	return nil
}

// setClosed atomically sets c.closed to 1 and returns if the call was the first
// to close the client
func (c *client) setClosed() bool {
	return atomic.CompareAndSwapUint32(&c.closed, 0, 1)
}

func (c *client) isClosed() bool {
	return atomic.LoadUint32(&c.closed) == 1
}

// cancelWithLocks calls and resets to nil the underlying context cancellation
// func if it is not nil. the client's lock must be held.
func (c *client) cancelWithLock() {
	if c.ctxCancel != nil {
		c.ctxCancel()
		c.ctxCancel = nil
	}
}

// val returns the value the leader should propose based on (1) a potentially
// empty override value and (2) the hostname of the caller (with a fallback to
// the DefaultHostname option).
func (c *client) val(override string) string {
	if override != "" {
		return override
	}

	if h, err := os.Hostname(); err == nil {
		return h
	}

	return c.opts.DefaultHostname()
}

func (c *client) newWatch() (xwatch.Watch, error) {
	_, w, err := c.wb.Watch()
	return w, err
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
		eid = defaultElectionSuffix
	}

	return fmt.Sprintf(
		keyFormat,
		servicePrefix(sid),
		eid)
}
