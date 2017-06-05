// Package leader provides functionality for etcd-backed leader elections.
package leader

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"fmt"

	"github.com/coreos/etcd/clientv3"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3x/watch"
)

const (
	leaderKeyPrefix   = "_ld"
	keySeparator      = "/"
	keyFormat         = "%s/%s"
	defaultHostname   = "default_hostname"
	leaderCallTimeout = 30 * time.Second
)

var (
	// ErrClientClosed indicates the election service client has been closed and
	// no more elections can be started.
	ErrClientClosed = errors.New("election client is closed")

	// ErrCampaignInProgress indicates a campaign cannot be started because one
	// is already in progress.
	ErrCampaignInProgress = errors.New("a campaign is already in progress")
)

type service struct {
	sync.RWMutex

	closed     uint32
	clients    map[string]*client
	opts       Options
	etcdClient *clientv3.Client
}

// NewService creates a new leader service client based on an etcd client.
func NewService(cli *clientv3.Client, opts Options) (services.LeaderService, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	return &service{
		clients:    make(map[string]*client),
		opts:       opts,
		etcdClient: cli,
	}, nil
}

// Close closes all underlying election clients and returns all errors
// encountered, if any.
func (s *service) Close() error {
	if atomic.CompareAndSwapUint32(&s.closed, 0, 1) {
		return s.closeClients()
	}

	return nil
}

func (s *service) isClosed() bool {
	return atomic.LoadUint32(&s.closed) == 1
}

func (s *service) closeClients() error {
	s.RLock()
	errC := make(chan error, len(s.clients))
	var wg sync.WaitGroup

	for _, cl := range s.clients {
		wg.Add(1)

		go func(cl *client) {
			if err := cl.Close(); err != nil {
				errC <- err
			}
			wg.Done()
		}(cl)
	}

	s.RUnlock()

	wg.Wait()
	close(errC)

	var errs []error
	for err := range errC {
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return newMultiError(errs...)
	}

	return nil
}

func (s *service) getOrCreateClient(electionID string) (*client, error) {
	s.RLock()
	client, ok := s.clients[electionID]
	s.RUnlock()
	if ok {
		return client, nil
	}

	clientNew, err := newClient(s.etcdClient, s.opts, electionID)
	if err != nil {
		return nil, err
	}

	s.Lock()
	defer s.Unlock()

	client, ok = s.clients[electionID]
	if ok {
		// another client was created between RLock and now, close new one
		go clientNew.Close()
		return client, nil
	}

	s.clients[electionID] = clientNew
	return clientNew, nil
}

func (s *service) Campaign(electionID string) (xwatch.Watch, error) {
	if s.isClosed() {
		return nil, ErrClientClosed
	}

	client, err := s.getOrCreateClient(electionID)
	if err != nil {
		return nil, err
	}

	return client.Campaign()
}

func (s *service) Resign(electionID string) error {
	if s.isClosed() {
		return ErrClientClosed
	}

	s.RLock()
	client, ok := s.clients[electionID]
	s.RUnlock()

	if !ok {
		return fmt.Errorf("no election with ID '%s' to resign", electionID)
	}

	return client.Resign()
}

func (s *service) Leader(electionID string) (string, error) {
	if s.isClosed() {
		return "", ErrClientClosed
	}

	// always create a client so we can check election statuses without
	// campaigning
	client, err := s.getOrCreateClient(electionID)
	if err != nil {
		return "", err
	}

	return client.Leader()
}
