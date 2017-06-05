// Package leader provides functionality for etcd-backed leader elections.
package leader

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/m3db/m3cluster/services"
	"golang.org/x/net/context"
)

const (
	leaderKeyPrefix   = "_ld"
	keySeparator      = "/"
	keyFormat         = "%s/%s"
	defaultHostname   = "default_hostname"
	leaderCallTimeout = 30 * time.Second

	// NB(mschalle): etcd's election API is based on prefixes, so if we have a
	// service-wide election and don't append some suffix, then a service-wide
	// election key would be a substring of a scoped sub-election (i.e. one with
	// an election ID). So we need to append a suffix to service-wide election
	// keys. We also want one that probably wouldn't conflict with a
	// sub-election ID
	svcElectionSuffix = "SVC_WIDE_ELECTION"
)

var (
	// ErrClientClosed indicates the election service client has been closed and
	// no more elections can be started.
	ErrClientClosed = errors.New("election client is closed")
	// ErrCampaignInProgress indicates a campaign cannot be started because one
	// is already in progress.
	ErrCampaignInProgress = errors.New("a campaign is already in progress")
)

// NewService creates a new leader service client based on an etcd client.
func NewService(cli *clientv3.Client, opts Options) (services.LeaderService, error) {
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

	return &client{
		election: election,
		session:  session,
		val:      opts.OverrideValue(),
	}, nil
}

type client struct {
	sync.Mutex

	ctxCancel context.CancelFunc
	election  *concurrency.Election
	session   *concurrency.Session
	val       string
	closed    uint32
}

func (c *client) Campaign() error {
	if c.isClosed() {
		return ErrClientClosed
	}

	ctx, cancel := context.WithCancel(context.Background())

	c.Lock()
	c.ctxCancel = cancel
	c.Unlock()

	// block until elected or error
	err := c.election.Campaign(ctx, c.val)

	c.Lock()
	c.ctxCancel = nil
	c.Unlock()

	return err
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

	return c.election.Resign(context.Background())
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
