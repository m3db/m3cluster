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
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/golang/protobuf/proto"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/retry"
	"golang.org/x/net/context"
)

var noopCancel func()

// NewStore creates a kv store based on etcd
func NewStore(etcd *clientv3.Client, opts Options) kv.Store {
	return &client{
		opts:       opts,
		kv:         etcd.KV,
		watcher:    etcd.Watcher,
		watchables: map[string]kv.ValueWatchable{},
		retrier:    xretry.NewRetrier(opts.RetryOptions()),
		logger:     opts.InstrumentsOptions().Logger(),
	}
}

type client struct {
	sync.RWMutex

	opts       Options
	kv         clientv3.KV
	watcher    clientv3.Watcher
	watchables map[string]kv.ValueWatchable
	retrier    xretry.Retrier
	logger     xlog.Logger
}

func (c *client) Get(key string) (kv.Value, error) {
	ctx, cancel := c.context()
	defer cancel()

	r, err := c.kv.Get(ctx, c.opts.KeyFn()(key))
	if err != nil {
		return nil, err
	}

	if r.Count == 0 {
		return nil, kv.ErrNotFound
	}

	if r.Count > 1 {
		return nil, fmt.Errorf("received %d values for key %s, expecting 1", r.Count, key)
	}

	return kv.NewValue(r.Kvs[0].Value, int(r.Kvs[0].Version)), nil
}

func (c *client) Watch(key string) (kv.ValueWatch, error) {
	c.Lock()
	watchable, ok := c.watchables[key]
	if !ok {
		watchChan := c.watcher.Watch(
			context.Background(),
			c.opts.KeyFn()(key),
			clientv3.WithProgressNotify(),
			// Receive initial notification once the watch channel is created
			clientv3.WithCreatedNotify(),
		)

		watchable = kv.NewValueWatchable()
		c.watchables[key] = watchable

		go func() {
			ticker := time.Tick(c.opts.WatchChanCheckInterval())

			for {
				select {
				case r := <-watchChan:
					c.processNotification(r, watchable, key)
				case <-ticker:
					c.RLock()
					numWatches := watchable.NumWatches()
					c.RUnlock()

					if numWatches != 0 {
						// there are still watches on this watchable, do nothing
						continue
					}

					if cleanedUp := c.tryCleanUp(key); cleanedUp {
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

func (c *client) tryCleanUp(key string) bool {
	c.Lock()
	defer c.Unlock()
	watchable, ok := c.watchables[key]
	if !ok {
		// not expect this to happen
		c.logger.Warnf("unexpected: key %s is already cleaned up", key)
		return true
	}

	if watchable.NumWatches() != 0 {
		// a new watch has subscribed to the watchable, do not clean up
		return false
	}

	watchable.Close()
	delete(c.watchables, key)
	return true
}

func (c *client) processNotification(r clientv3.WatchResponse, w kv.ValueWatchable, key string) {
	err := r.Err()
	if err != nil {
		c.logger.Errorf("received error on watch channel: %v", err)
	}

	// we need retry here because if Get() failed on an watch update,
	// it has to wait 10 mins to be notified to try again
	err = c.retrier.Attempt(func() error {
		return c.update(w, key)
	})

	if err != nil {
		c.logger.Errorf("received notification for key %s, but failed to get value: %v", key, err)
	}
}

func (c *client) update(w kv.ValueWatchable, key string) error {
	v, err := c.Get(key)
	if err != nil {
		return err
	}

	curValue := w.Get()
	if curValue != nil && curValue.Version() >= v.Version() {
		return nil
	}

	return w.Update(v)
}

func (c *client) Set(key string, v proto.Message) (int, error) {
	ctx, cancel := c.context()
	defer cancel()

	value, err := proto.Marshal(v)
	if err != nil {
		return 0, err
	}

	r, err := c.kv.Put(ctx, c.opts.KeyFn()(key), string(value), clientv3.WithPrevKV())
	if err != nil {
		return 0, err
	}

	// if there is no prev kv, means this is the first version of the key
	if r.PrevKv == nil {
		return 1, nil
	}

	return int(r.PrevKv.Version + 1), nil
}

func (c *client) SetIfNotExists(key string, v proto.Message) (int, error) {
	ctx, cancel := c.context()
	defer cancel()

	value, err := proto.Marshal(v)
	if err != nil {
		return 0, err
	}

	key = c.opts.KeyFn()(key)
	r, err := c.kv.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(key), "=", 0)).
		Then(clientv3.OpPut(key, string(value))).
		Commit()
	if err != nil {
		return 0, err
	}
	if !r.Succeeded {
		return 0, kv.ErrAlreadyExists
	}
	return 1, nil
}

func (c *client) CheckAndSet(key string, version int, v proto.Message) (int, error) {
	ctx, cancel := c.context()
	defer cancel()

	value, err := proto.Marshal(v)
	if err != nil {
		return 0, err
	}

	key = c.opts.KeyFn()(key)
	r, err := c.kv.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(key), "=", version)).
		Then(clientv3.OpPut(key, string(value))).
		Commit()
	if err != nil {
		return 0, err
	}
	if !r.Succeeded {
		return 0, kv.ErrVersionMismatch
	}

	return version + 1, nil
}

func (c *client) context() (context.Context, context.CancelFunc) {
	ctx := context.Background()
	cancel := noopCancel
	if c.opts.RequestTimeout() > 0 {
		ctx, cancel = context.WithTimeout(ctx, c.opts.RequestTimeout())
	}

	return ctx, cancel
}
