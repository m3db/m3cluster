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

package placement

import (
	"errors"
	"sync"

	"github.com/m3db/m3cluster/generated/proto/placementpb"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/kv/util/runtime"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/log"
)

var (
	errNilValue                      = errors.New("nil value received")
	errPlacementWatcherIsNotWatching = errors.New("placement watcher is not watching")
	errPlacementWatcherIsWatching    = errors.New("placement watcher is watching")
)

type placementWatcherState int

const (
	placementWatcherNotWatching placementWatcherState = iota
	placementWatcherWatching
)

type stagedPlacementWatcher struct {
	sync.RWMutex
	runtime.Value

	nowFn         clock.NowFn
	key           string
	placementOpts ActiveStagedPlacementOptions
	doneFn        DoneFn
	logger        log.Logger

	state     placementWatcherState
	proto     *placementpb.PlacementSnapshots
	placement ActiveStagedPlacement
}

// NewStagedPlacementWatcher creates a new staged placement watcher.
func NewStagedPlacementWatcher(opts StagedPlacementWatcherOptions) StagedPlacementWatcher {
	watcher := &stagedPlacementWatcher{
		key:           opts.StagedPlacementKey(),
		logger:        opts.InstrumentOptions().Logger(),
		nowFn:         opts.ClockOptions().NowFn(),
		placementOpts: opts.ActiveStagedPlacementOptions(),
		proto:         &placementpb.PlacementSnapshots{},
	}
	watcher.doneFn = watcher.onActiveStagedPlacementDone

	updatableFn := func() (runtime.Updatable, error) {
		return opts.StagedPlacementStore().Watch(watcher.key)
	}
	getFn := func(value runtime.Updatable) (interface{}, error) {
		return value.(kv.ValueWatch).Get(), nil
	}
	valueOpts := runtime.NewOptions().
		SetInstrumentOptions(opts.InstrumentOptions()).
		SetInitWatchTimeout(opts.InitWatchTimeout()).
		SetUpdatableFn(updatableFn).
		SetGetFn(getFn).
		SetProcessFn(watcher.process)
	watcher.Value = runtime.NewValue(valueOpts)
	return watcher
}

func (t *stagedPlacementWatcher) Watch() error {
	t.Lock()
	if t.state != placementWatcherNotWatching {
		t.Unlock()
		return errPlacementWatcherIsWatching
	}
	t.state = placementWatcherWatching
	t.Unlock()

	// NB(xichen): we watch the placementWatcher updates outside the lock because
	// otherwise the initial update will trigger the process() callback,
	// which attempts to acquire the same lock, causing a deadlock.
	return t.Value.Watch()
}

func (t *stagedPlacementWatcher) ActiveStagedPlacement() (ActiveStagedPlacement, DoneFn, error) {
	t.RLock()
	if t.state != placementWatcherWatching {
		t.RUnlock()
		return nil, nil, errPlacementWatcherIsNotWatching
	}
	return t.placement, t.doneFn, nil
}

func (t *stagedPlacementWatcher) Unwatch() error {
	t.Lock()
	if t.state != placementWatcherWatching {
		t.Unlock()
		return errPlacementWatcherIsNotWatching
	}
	t.state = placementWatcherNotWatching
	if t.placement != nil {
		t.placement.Close()
	}
	t.placement = nil
	t.Unlock()

	// NB(xichen): we unwatch the updates outside the lock to avoid deadlock
	// due to placementWatcher contending for the runtime value lock and the
	// runtime updating goroutine attempting to acquire placementWatcher lock.
	t.Value.Unwatch()
	return nil
}

func (t *stagedPlacementWatcher) onActiveStagedPlacementDone() { t.RUnlock() }

func (t *stagedPlacementWatcher) toStagedPlacementWithLock(value kv.Value) (StagedPlacement, error) {
	if t.state != placementWatcherWatching {
		return nil, errPlacementWatcherIsNotWatching
	}
	t.proto.Reset()
	if err := value.Unmarshal(t.proto); err != nil {
		return nil, err
	}
	version := value.Version()
	return NewStagedPlacementFromProto(version, t.proto, t.placementOpts)
}

func (t *stagedPlacementWatcher) process(update interface{}) error {
	t.Lock()
	defer t.Unlock()

	if t.state != placementWatcherWatching {
		return errPlacementWatcherIsNotWatching
	}
	if update == nil {
		return errNilValue
	}
	value := update.(kv.Value)
	t.logger.Infof("processing update from kv for key %s with version %d", t.key, value.Version())
	ps, err := t.toStagedPlacementWithLock(value)
	if err != nil {
		t.logger.Errorf("could not convert kv update to staged placement, %v", err)
		return err
	}
	placement := ps.ActiveStagedPlacement(t.nowFn().UnixNano())
	if t.placement != nil {
		t.placement.Close()
	}
	t.placement = placement
	return nil
}
