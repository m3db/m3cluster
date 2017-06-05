package leader

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3x/watch"

	"golang.org/x/net/context"
)

// CampaignState describes the state of a campaign as its relates to the
// caller's leadership.
type CampaignState int

const (
	// CampaignUnstarted indicates the caller has not yet called Campaign.
	CampaignUnstarted CampaignState = iota

	// CampaignFollower indicates the caller has called Campaign but has not yet
	// been elected.
	CampaignFollower

	// CampaignLeader indicates the caller has called Campaign and was elected.
	CampaignLeader

	// CampaignError indicates the call to Campaign returned an error.
	CampaignError

	// CampaignClosed indicates the campaign has been closed.
	CampaignClosed
)

var (
	// ErrSessionExpired is returned when a client's session (etcd lease) is no
	// longer being refreshed for any reason (due to expiration, error state,
	// etc.).
	ErrSessionExpired = errors.New("election client session (lease) expired")
)

// NB(mschalle): when an etcd leader failover occurs, all current leases have
// their TTLs refreshed: https://github.com/coreos/etcd/issues/2660

type client struct {
	sync.Mutex

	ctxCancel context.CancelFunc
	election  *concurrency.Election
	session   *concurrency.Session
	val       string
	closed    uint32
	wb        xwatch.Watchable
}

// newClient returns an instance of an client client bound to a single election.
func newClient(cli *clientv3.Client, opts Options) (*client, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	var sessionOpts []concurrency.SessionOption
	if ttl := opts.TTL(); ttl != 0 {
		sessionOpts = append(sessionOpts, concurrency.WithTTL(ttl))
	}

	session, err := concurrency.NewSession(cli, sessionOpts...)
	if err != nil {
		return nil, err
	}

	election := concurrency.NewElection(session, servicePrefix(opts.ServiceID()))

	wb := xwatch.NewWatchable()
	wb.Update(CampaignUnstarted)

	return &client{
		election: election,
		session:  session,
		val:      opts.OverrideValue(),
		wb:       wb,
	}, nil
}

func (c *client) Campaign() (xwatch.Watch, error) {
	if c.isClosed() {
		return nil, ErrClientClosed
	}

	ctx, cancel := context.WithCancel(context.Background())

	c.Lock()
	c.ctxCancel = cancel
	c.Unlock()

	go func() {
		c.wb.Update(CampaignFollower)
		// blocks until elected or error
		err := c.election.Campaign(ctx, c.val)
		if err != nil {
			c.wb.Update(err)
		}

		select {
		case <-c.session.Done():
			c.wb.Update(ErrSessionExpired)
		default:
			c.wb.Update(CampaignLeader)
		}

		c.Lock()
		c.ctxCancel = nil
		c.Unlock()
	}()

	_, w, err := c.wb.Watch()
	if err != nil {
		return nil, err
	}

	return w, nil
}

func (c *client) Resign() error {
	if c.isClosed() {
		return ErrClientClosed
	}

	c.Lock()
	defer c.Unlock()

	// if we're not the leader but still campaigning, cancelling the context
	// will stop the campaign
	c.cancelWithLock()

	// TODO(mschalle): resign context timeout?
	if err := c.election.Resign(context.Background()); err != nil {
		c.wb.Update(err)
		return err
	}

	c.wb.Update(CampaignFollower)
	return nil
}

func (c *client) Leader() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), leaderCallTimeout)
	defer cancel()
	return c.election.Leader(ctx)
}

// Close closes the election service client entirely. No more campaigns can be
// started and any outstanding campaigns are closed.
func (c *client) Close() error {
	if c.close() {
		c.Lock()
		c.cancelWithLock()
		c.Unlock()

		c.wb.Update(CampaignClosed)
		c.wb.Close()
		return c.session.Close()
	}

	return nil
}

// close atomically sets c.closed to 1 and returns if the call was the first to
// close the client
func (c *client) close() bool {
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

func electionPrefix(opts Options) string {
	eid := opts.ElectionOpts().ElectionID()
	if eid == "" {
		eid = svcElectionSuffix
	}

	return fmt.Sprintf(
		keyFormat,
		servicePrefix(opts.ServiceID()),
		eid)
}
