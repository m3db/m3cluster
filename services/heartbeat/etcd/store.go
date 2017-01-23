// Copyright (c) 2016 Uber Technologies, Inc.
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

package etcd

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/m3db/m3cluster/services/heartbeat"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/retry"
	"github.com/m3db/m3x/watch"
	"github.com/uber-go/tally"
	"golang.org/x/net/context"
)

const (
	heartbeatKeyPrefix = "_hb"
	keySeparator       = "/"
	keyFormat          = "%s/%s"
)

var noopCancel func()

// NewStore creates a heartbeat store based on etcd
func NewStore(c *clientv3.Client, opts Options) heartbeat.Store {
	scope := opts.InstrumentsOptions().MetricsScope()

	return &client{
		leases:     make(map[string]clientv3.LeaseID),
		watchables: make(map[string]xwatch.Watchable),
		opts:       opts,
		logger:     opts.InstrumentsOptions().Logger(),
		retrier:    xretry.NewRetrier(opts.RetryOptions()),
		m: clientMetrics{
			etcdGetError:    scope.Counter("etcd-get-error"),
			etcdPutError:    scope.Counter("etcd-put-error"),
			etcdLeaseError:  scope.Counter("etcd-lease-error"),
			etcdWatchCreate: scope.Counter("etcd-watch-create"),
			etcdWatchError:  scope.Counter("etcd-watch-error"),
			etcdWatchReset:  scope.Counter("etcd-watch-reset"),
		},

		l:       c.Lease,
		kv:      c.KV,
		watcher: c.Watcher,
	}
}

type client struct {
	sync.RWMutex

	leases     map[string]clientv3.LeaseID
	watchables map[string]xwatch.Watchable
	opts       Options
	logger     xlog.Logger
	retrier    xretry.Retrier
	m          clientMetrics

	l       clientv3.Lease
	kv      clientv3.KV
	watcher clientv3.Watcher
}

type clientMetrics struct {
	etcdGetError    tally.Counter
	etcdPutError    tally.Counter
	etcdLeaseError  tally.Counter
	etcdWatchCreate tally.Counter
	etcdWatchError  tally.Counter
	etcdWatchReset  tally.Counter
}

func (c *client) Heartbeat(service, instance string, ttl time.Duration) error {
	key := leaseKey(service, instance, ttl)

	c.RLock()
	leaseID, ok := c.leases[key]
	c.RUnlock()

	if ok {
		ctx, cancel := c.context()
		defer cancel()

		_, err := c.l.KeepAliveOnce(ctx, leaseID)
		// if err != nil, it could because the old lease has already timedout
		// on the server side, we need to try a new lease.
		if err == nil {
			return nil
		}
	}

	ctx, cancel := c.context()
	defer cancel()

	resp, err := c.l.Grant(ctx, int64(ttl/time.Second))
	if err != nil {
		c.m.etcdLeaseError.Inc(1)
		return err
	}

	ctx, cancel = c.context()
	defer cancel()

	_, err = c.kv.Put(
		ctx,
		heartbeatKey(service, instance),
		"",
		clientv3.WithLease(resp.ID),
	)
	if err != nil {
		c.m.etcdPutError.Inc(1)
		return err
	}

	c.Lock()
	c.leases[key] = resp.ID
	c.Unlock()

	return nil
}

func (c *client) Get(service string) ([]string, error) {
	ctx, cancel := c.context()
	defer cancel()

	gr, err := c.kv.Get(ctx, servicePrefix(service), clientv3.WithPrefix())
	if err != nil {
		c.m.etcdGetError.Inc(1)
		return nil, err
	}

	r := make([]string, len(gr.Kvs))
	for i, kv := range gr.Kvs {
		r[i] = instanceFromKey(string(kv.Key), service)
	}
	return r, nil
}

func (c *client) Watch(service string) (xwatch.Watch, error) {
	c.Lock()
	watchable, ok := c.watchables[service]

	if !ok {
		watchChan := c.watchChan(service)
		c.m.etcdWatchCreate.Inc(1)

		watchable = xwatch.NewWatchable()
		c.watchables[service] = watchable

		go func() {
			ticker := time.Tick(c.opts.WatchChanCheckInterval())

			for {
				select {
				case r, ok := <-watchChan:
					if ok {
						c.processNotification(r, watchable, service)
					} else {
						c.logger.Warnf("etcd watch channel closed on service %s, recreating a watch channel", service)

						// avoid recreating watch channel too frequently
						time.Sleep(c.opts.WatchChanResetInterval())

						watchChan = c.watchChan(service)
						c.m.etcdWatchReset.Inc(1)
					}
				case <-ticker:
					c.RLock()
					numWatches := watchable.NumWatches()
					c.RUnlock()

					if numWatches != 0 {
						// there are still watches on this watchable, do nothing
						continue
					}

					if cleanedUp := c.tryCleanUp(service); cleanedUp {
						return
					}
				}
			}
		}()
	}
	c.Unlock()
	_, w, err := watchable.Watch()
	return w, err
}

func (c *client) watchChan(service string) clientv3.WatchChan {
	return c.watcher.Watch(
		context.Background(),
		servicePrefix(service),
		// WithPrefix so that the watch will receive any changes
		// from the instances under the service
		clientv3.WithPrefix(),
		// periodically (appx every 10 mins) checks for the latest data
		// with or without any update notification
		clientv3.WithProgressNotify(),
		// receive initial notification once the watch channel is created
		clientv3.WithCreatedNotify(),
	)
}

func (c *client) tryCleanUp(service string) bool {
	c.Lock()
	defer c.Unlock()
	watchable, ok := c.watchables[service]
	if !ok {
		// not expect this to happen
		c.logger.Warnf("unexpected: watches on service %s is already cleaned up", service)
		return true
	}

	if watchable.NumWatches() != 0 {
		// a new watch has subscribed to the watchable, do not clean up
		return false
	}

	watchable.Close()
	delete(c.watchables, service)
	return true
}

func (c *client) processNotification(r clientv3.WatchResponse, w xwatch.Watchable, service string) {
	err := r.Err()
	if err != nil {
		c.logger.Errorf("received error on watch channel: %v", err)
		c.m.etcdWatchError.Inc(1)
	}
	// we need retry here because if Get() failed on an watch update,
	// it has to wait 10 mins to be notified to try again
	if err = c.retrier.Attempt(func() error {
		return c.update(w, service)
	}); err != nil {
		c.logger.Errorf("received updates for service %s, but failed to get value: %v", service, err)
	}
}

func (c *client) update(w xwatch.Watchable, service string) error {
	newValue, err := c.Get(service)
	if err != nil {
		return err
	}

	w.Update(newValue)

	return nil
}

func (c *client) context() (context.Context, context.CancelFunc) {
	ctx := context.Background()
	cancel := noopCancel
	if c.opts.RequestTimeout() > 0 {
		ctx, cancel = context.WithTimeout(ctx, c.opts.RequestTimeout())
	}

	return ctx, cancel
}

func heartbeatKey(service, instance string) string {
	return fmt.Sprintf(keyFormat, servicePrefix(service), instance)
}

func instanceFromKey(key, service string) string {
	return strings.TrimPrefix(
		strings.TrimPrefix(key, servicePrefix(service)),
		keySeparator,
	)
}

func servicePrefix(service string) string {
	return fmt.Sprintf(keyFormat, heartbeatKeyPrefix, service)
}

func leaseKey(service, instance string, ttl time.Duration) string {
	return fmt.Sprintf(keyFormat, heartbeatKey(service, instance), ttl.String())
}
