package leader

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3x/retry"
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

	ctxCancel context.CancelFunc
	election  *concurrency.Election
	session   *concurrency.Session
	opts      services.ElectionOptions

	// state needed for resetElection()
	etcdClient  *clientv3.Client
	sessionOpts []concurrency.SessionOption
	clientOpts  Options
	electionID  string

	closed      uint32
	campaigning uint32
	campaignVal string

	retrier xretry.Retrier
	wb      xwatch.Watchable
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

	cl := &client{
		opts:        opts.ElectionOpts(),
		wb:          xwatch.NewWatchable(),
		etcdClient:  cli,
		sessionOpts: sessionOpts,
		clientOpts:  opts,
		electionID:  electionID,
	}

	cl.wb.Update(newCampaignStatus(CampaignFollower))

	if err := cl.resetElection(); err != nil {
		return nil, err
	}

	return cl, nil
}

// func called when session expires / is orphaned for any reason and needs
// to be recreated
func (c *client) resetElection() error {
	if c.isClosed() {
		return errClientClosed
	}

	session, err := concurrency.NewSession(c.etcdClient, c.sessionOpts...)
	if err != nil {
		return err
	}

	election := concurrency.NewElection(session, electionPrefix(c.clientOpts.ServiceID(), c.electionID))
	c.Lock()
	c.session = session
	c.election = election
	c.Unlock()
	return nil
}

func (c *client) campaign(override string, opts services.CampaignOptions) (xwatch.Watch, error) {
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

	retrier := xretry.NewRetrier(opts.RetryOptions())
	go c.campaignWhile(retrier, proposeVal)

	return c.newWatch()
}

func (c *client) campaignWhile(retrier xretry.Retrier, proposeVal string) {
	continueFn := xretry.ContinueFn(func(int) bool {
		return !c.isClosed()
	})

	fn := func() error {
		return c.campaignOnce(proposeVal)
	}

	retrier.AttemptWhile(continueFn, fn)
}

func (c *client) campaignOnce(proposeVal string) error {
	ctx, cancel := context.WithCancel(context.Background())

	c.Lock()
	c.campaignVal = proposeVal
	c.ctxCancel = cancel
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

	// if the user cancelled the context externally (i.e called resign() or
	// close()) then we don't want to retry the campaign
	select {
	case <-ctx.Done():
		return xretry.NonRetryableError(context.Canceled)
	default:
		cancel()
	}

	c.Lock()
	c.ctxCancel = nil
	c.Unlock()

	// if session timed out we need to call the reset func to create a new one
	// (gets a new lease from etcd and creates an election client with) that
	// lease attached
	if failed {
		c.resetElection()
		return xretry.RetryableError(ErrSessionExpired)
	}

	return nil
}

func (c *client) resign() error {
	if c.isClosed() {
		return errClientClosed
	}

	c.Lock()
	// if we're not the leader but still campaigning, cancelling the context
	// will stop the campaign
	c.cancelWithLock()
	c.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), c.opts.ResignTimeout())
	defer cancel()
	if err := c.election.Resign(ctx); err != nil {
		c.wb.Update(newErrCampaignStatus(err))
		return err
	}

	c.Lock()
	c.resetCampaignWithLock()
	c.Unlock()
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

	return c.opts.Hostname()
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
