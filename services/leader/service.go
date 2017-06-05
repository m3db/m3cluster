// Package leader provides functionality for etcd-backed leader elections.
package leader

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

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

type service struct {
	sync.RWMutex

	closed  uint32
	clients map[string]*client
	opts    Options
}

// NewService creates a new leader service client based on an etcd client.
func NewService(cli *clientv3.Client, opts Options) (services.LeaderService, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	return &service{
		clients: make(map[string]*client),
		opts:    opts,
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
		return newMultiError(errs)
	}

	return nil
}

func (s *service) Campaign(electionID string) (xwatch.Watch, error) {
	panic("not implemented")
}

func (s *service) Resign(electionID string) error {
	panic("not implemented")
}

func (s *service) Leader(electionID string) (string, error) {
	panic("not implemented")
}
