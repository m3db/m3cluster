// Package leader provides functionality for etcd-backed leader elections.
package leader

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"os"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/m3db/m3cluster/services"
	"golang.org/x/net/context"
)

const (
	leaderKeyPrefix = "_ld"
	keySeparator    = "/"
	keyFormat       = "%s/%s"
	defaultHostname = "default_hostname"
)

var (
	errNilElection = errors.New("nil election was returned")
)

// NewService creates a new leader service client based on an etcd client.
func NewService(cli *clientv3.Client, opts Options) (services.LeaderService, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	sessionOpts := []concurrency.SessionOption{}
	if ttl := opts.TTL(); ttl != 0 {
		sessionOpts = append(sessionOpts, concurrency.WithTTL(ttl))
	}

	session, err := concurrency.NewSession(cli, sessionOpts...)
	if err != nil {
		return nil, err
	}

	election := concurrency.NewElection(session, servicePrefix(opts.ServiceID()))
	if election == nil {
		return nil, errNilElection
	}

	c := &client{
		election: election,
		session:  session,
		val:      electionValue(opts),
	}

	return c, nil
}

type client struct {
	sync.Mutex

	ctx       context.Context
	ctxCancel context.CancelFunc
	election  *concurrency.Election
	session   *concurrency.Session
	val       string
}

func (c *client) Campaign() error {
	ctx, cancel := context.WithCancel(context.Background())

	c.Lock()
	c.ctx = ctx
	c.ctxCancel = cancel
	c.Unlock()

	// block until elected or error
	err := c.election.Campaign(ctx, c.val)

	c.Lock()
	c.ctx = nil
	c.ctxCancel = nil
	c.Unlock()

	return err
}

func (c *client) Resign() error {
	c.Lock()
	defer c.Unlock()

	// if we're not the leader but still campaigning, cancelling the context
	// will stop the campaign
	if c.ctx != nil && c.ctxCancel != nil {
		c.ctxCancel()
		c.ctx = nil
		c.ctxCancel = nil
	}

	return c.election.Resign(context.Background())
}

func (c *client) Leader() (string, error) {
	// TODO(mschalle): make configurable. need greater than client's DialTimeout
	// to be resilient to node failures
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return c.election.Leader(ctx)
}

// Close closes the election service client entirely. No more campaigns can be
// started and any outstanding campaigns are closed.
func (c *client) Close() error {
	return c.session.Close()
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

// electionValue returns the OverrideValue from opts if it is non-empty,
// otherwise returns the current hostname by default.
func electionValue(opts Options) string {
	if o := opts.OverrideValue(); o != "" {
		return o
	}

	h, err := os.Hostname()
	if err != nil {
		return defaultHostname
	}

	return h
}
