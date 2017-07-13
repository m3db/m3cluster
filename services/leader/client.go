// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package leader

import (
	"errors"
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

// Appended to elections with an empty string for electionID to make it easier
// for user to debug etcd keys.
const defaultElectionID = "default"

var (
	// ErrNoLeader is returned when a call to Leader() is made to an election
	// with no leader. We duplicate this error so the user doesn't have to
	// import etcd's concurrency package in order to check the cause of the
	// error.
	ErrNoLeader = concurrency.ErrElectionNoLeader

	// ErrCampaignInProgress is returned when a call to Campaign() is made while
	// the caller is either already (a) campaigning or (b) the leader.
	ErrCampaignInProgress = errors.New("campaign in progress")
)

// NB(mschalle): when an etcd leader failover occurs, all current leases have
// their TTLs refreshed: https://github.com/coreos/etcd/issues/2660

type client struct {
	sync.RWMutex

	client      *election.Client
	opts        services.ElectionOptions
	cancelFn    context.CancelFunc
	resignCh    chan struct{}
	campaigning uint32
	closed      uint32
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
		client:   ec,
		opts:     opts.ElectionOpts(),
		resignCh: make(chan struct{}),
	}, nil
}

func (c *client) campaign(opts services.CampaignOptions) (<-chan campaign.Status, error) {
	if c.isClosed() {
		return nil, errClientClosed
	}

	if !c.startCampaign() {
		return nil, ErrCampaignInProgress
	}

	ctx, cancel := context.WithCancel(context.Background())
	c.Lock()
	c.cancelFn = cancel
	c.Unlock()

	proposeVal := c.val(opts.LeaderValue())

	// buffer 1 to not block initial follower update
	sc := make(chan campaign.Status, 1)

	sc <- campaign.NewStatus(campaign.Follower)

	go func() {
		defer func() {
			close(sc)
			cancel()
			c.resetCampaign()
		}()

		// Campaign blocks until elected. Once we are elected, we get a channel
		// that's closed if our session dies.
		ch, err := c.client.Campaign(ctx, proposeVal)
		if err != nil {
			sc <- campaign.NewErrorStatus(err)
			return
		}

		sc <- campaign.NewStatus(campaign.Leader)
		select {
		case <-ch:
			sc <- campaign.NewErrorStatus(election.ErrSessionExpired)
		case <-c.resignCh:
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
	if c.cancelFn != nil {
		c.cancelFn()
		c.cancelFn = nil
	}
	c.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), c.opts.ResignTimeout())
	defer cancel()
	if err := c.client.Resign(ctx); err != nil {
		return err
	}

	c.resetCampaign()

	// if successfully resigned and there was a campaign in Leader state cancel
	// it
	select {
	case c.resignCh <- struct{}{}:
	default:
	}

	return nil
}

func (c *client) leader() (string, error) {
	if c.isClosed() {
		return "", errClientClosed
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.opts.LeaderTimeout())
	defer cancel()
	ld, err := c.client.Leader(ctx)
	if err == concurrency.ErrElectionNoLeader {
		return ld, ErrNoLeader
	}
	return ld, err
}

func (c *client) startCampaign() bool {
	return atomic.CompareAndSwapUint32(&c.campaigning, 0, 1)
}

func (c *client) resetCampaign() {
	atomic.StoreUint32(&c.campaigning, 0)
}

// Close closes the election service client entirely. No more campaigns can be
// started and any outstanding campaigns are closed.
func (c *client) close() error {
	atomic.StoreUint32(&c.closed, 1)
	return c.client.Close()
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
