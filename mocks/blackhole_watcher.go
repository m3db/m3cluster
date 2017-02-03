package mocks

import (
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"golang.org/x/net/context"
)

// watcher mocks an etcd client that just blackholes a few watch requests
type watcher struct {
	sync.Mutex

	failed    int
	failTotal int
	c         *clientv3.Client
}

// NewBlackholeWatcher returns a watcher that mimics blackholing
func NewBlackholeWatcher(failTotal int, c *clientv3.Client) clientv3.Watcher {
	return &watcher{
		failed:    0,
		failTotal: failTotal,
		c:         c,
	}
}

// Watch is implementing etcd clientv3 Watcher interface
func (m *watcher) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	m.Lock()

	if m.failed < m.failTotal {
		m.failed++
		m.Unlock()

		time.Sleep(time.Minute)
		return nil
	}
	m.Unlock()

	return m.c.Watch(ctx, key, opts...)
}

// Close is implementing etcd clientv3 Watcher interface
func (m *watcher) Close() error {
	return m.c.Close()
}
