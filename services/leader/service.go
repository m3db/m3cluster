/*
Package leader provides functionality for etcd-backed leader elections.

The following diagram illustrates the states of an election:

                         FOLLOWER<---------------+
                            +                    ^
                            |                    |
          Campaign() blocks +-------------------->
                            |     Resign()       |
                            |                    |
                            v                    |
                   +--------+                    |
                   |        |                    |
                   |        | Campaign() OK      |
                   |        |                    |
                   |        |                    |
   Campaign() Err  |        v      Resign()      |
                   |      LEADER+--------------->+
                   |        +                    ^
                   |        | Lose session       |
                   |        |                    |
                   |        v                    |
                   +----->ERROR+-----------------+
                                Campaign() again

An election starts in FOLLOWER state when a call to Campaign() is first made.
The underlying call to etcd will block until the client is either (a) elected
(in which case the election will be in LEADER state) or (b) an error is
encountered (in which case election will be in ERROR state). If an election is
in LEADER state but the session expires in the background it will transition to
ERROR state (and Campaign() will need to be called again to progress). If an
election is in LEADER state and the user calls Resign() it will transition back
to FOLLOWER state. Finally, if an election is in FOLLOWER state and a blocking
call to Campaign() is ongoing and the user calls Resign(), the campaign will be
cancelled and the election back in FOLLOWER state.

The leader package requires some care to be used correctly. Calls to Resign()
should ideally be done from the same goroutine as the call to Campaign().
Multiple simultaneous calls to Campaign() are not guaranteed to succeed.
Specifically, while a call to Campaign() will update the election value if the
caller is the leader, a second call to Campaign() while a first is blocking will
lock the underlying election client, causing further calls to Resign() to block
until the client is unlocked. To avoid this, don't call Campaign() for a second
time without first calling Resign().
*/
package leader

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/coreos/etcd/clientv3"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/leader/campaign"
	"github.com/m3db/m3x/errors"
)

const (
	leaderKeyPrefix = "_ld"
	keySeparator    = "/"
	keyFormat       = "%s/%s"
)

var (
	// errClientClosed indicates the election service client has been closed and
	// no more elections can be started.
	errClientClosed = errors.New("election client is closed")
)

type multiClient struct {
	sync.RWMutex

	closed     uint32
	clients    map[string]*clientEntry
	opts       Options
	etcdClient *clientv3.Client
}

// clientEntry stores a cached client as well as the TTL it was created with so
// that a user will receive an error if they try to create a new client with a
// different TTL
type clientEntry struct {
	client *client
	ttl    int
}

// NewService creates a new leader service client based on an etcd client.
func NewService(cli *clientv3.Client, opts Options) (services.LeaderService, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	return &multiClient{
		clients:    make(map[string]*clientEntry),
		opts:       opts,
		etcdClient: cli,
	}, nil
}

// Close closes all underlying election clients and returns all errors
// encountered, if any.
func (s *multiClient) Close() error {
	if atomic.CompareAndSwapUint32(&s.closed, 0, 1) {
		return s.closeClients()
	}

	return nil
}

func (s *multiClient) isClosed() bool {
	return atomic.LoadUint32(&s.closed) == 1
}

func (s *multiClient) closeClients() error {
	s.RLock()
	errC := make(chan error, len(s.clients))
	var wg sync.WaitGroup

	for _, cl := range s.clients {
		wg.Add(1)

		go func(cl *client) {
			if err := cl.close(); err != nil {
				errC <- err
			}
			wg.Done()
		}(cl.client)
	}

	s.RUnlock()

	wg.Wait()
	close(errC)

	merr := xerrors.NewMultiError()
	for err := range errC {
		if err != nil {
			merr = merr.Add(err)
		}
	}

	if merr.NumErrors() > 0 {
		return merr
	}

	return nil
}

func (s *multiClient) getOrCreateClient(electionID string, ttl int) (*client, error) {
	const errTTLFmt = "cannot create client with ttl=(%d), already created with ttl=(%d)"

	s.RLock()
	ce, ok := s.clients[electionID]
	s.RUnlock()
	if ok {
		if ce.ttl != ttl {
			return nil, fmt.Errorf(errTTLFmt, ttl, ce.ttl)
		}
		return ce.client, nil
	}

	clientNew, err := newClient(s.etcdClient, s.opts, electionID, ttl)
	if err != nil {
		return nil, err
	}

	s.Lock()
	defer s.Unlock()

	ce, ok = s.clients[electionID]
	if ok {
		// another client was created between RLock and now, close new one
		go clientNew.close()

		if ce.ttl != ttl {
			return nil, fmt.Errorf(errTTLFmt, ttl, ce.ttl)
		}

		return ce.client, nil
	}

	s.clients[electionID] = &clientEntry{client: clientNew, ttl: ttl}
	return clientNew, nil
}

func (s *multiClient) Campaign(electionID string, ttl int, opts services.CampaignOptions) (<-chan campaign.Status, error) {
	if s.isClosed() {
		return nil, errClientClosed
	}

	client, err := s.getOrCreateClient(electionID, ttl)
	if err != nil {
		return nil, err
	}

	if opts == nil {
		opts = services.NewCampaignOptions()
	}

	return client.campaign(opts)
}

func (s *multiClient) Resign(electionID string) error {
	if s.isClosed() {
		return errClientClosed
	}

	s.RLock()
	ce, ok := s.clients[electionID]
	s.RUnlock()

	if !ok {
		return fmt.Errorf("no election with ID '%s' to resign", electionID)
	}

	return ce.client.resign()
}

func (s *multiClient) Leader(electionID string, ttl int) (string, error) {
	if s.isClosed() {
		return "", errClientClosed
	}

	// always create a client so we can check election statuses without
	// campaigning
	client, err := s.getOrCreateClient(electionID, ttl)
	if err != nil {
		return "", err
	}

	return client.leader()
}
