// Package leader provides functionality for etcd-backed leader elections.
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
// that a user will receive and error if they try to create a new client with a
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
	s.RLock()
	ce, ok := s.clients[electionID]
	s.RUnlock()
	if ok {
		if ce.ttl != ttl {
			return nil, fmt.Errorf("cannot create client with ttl=(%d), already created with ttl=(%d)", ttl, ce.ttl)
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

	return client.campaign(opts.LeaderValue(), opts)
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
