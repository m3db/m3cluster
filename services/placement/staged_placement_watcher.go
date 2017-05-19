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

	schema "github.com/m3db/m3cluster/generated/proto/placement"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/kv/util/runtime"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3x/clock"
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
	placementOpts services.ActiveStagedPlacementOptions
	doneFn        services.DoneFn

	state     placementWatcherState
	proto     *schema.PlacementSnapshots
	placement services.ActiveStagedPlacement
}

// NewStagedPlacementWatcher creates a new staged placement watcher.
func NewStagedPlacementWatcher(opts services.StagedPlacementWatcherOptions) services.StagedPlacementWatcher {
	watcher := &stagedPlacementWatcher{
		nowFn:         opts.ClockOptions().NowFn(),
		placementOpts: opts.ActiveStagedPlacementOptions(),
		proto:         &schema.PlacementSnapshots{},
	}
	watcher.doneFn = watcher.onActiveStagedPlacementDone

	valueOpts := runtime.NewOptions().
		SetInstrumentOptions(opts.InstrumentOptions()).
		SetInitWatchTimeout(opts.InitWatchTimeout()).
		SetKVStore(opts.StagedPlacementStore()).
		SetUnmarshalFn(watcher.toStagedPlacement).
		SetProcessFn(watcher.process)
	watcher.Value = runtime.NewValue(opts.StagedPlacementKey(), valueOpts)
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

func (t *stagedPlacementWatcher) ActiveStagedPlacement() (services.ActiveStagedPlacement, services.DoneFn, error) {
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

func (t *stagedPlacementWatcher) toStagedPlacement(value kv.Value) (interface{}, error) {
	t.Lock()
	defer t.Unlock()

	if t.state != placementWatcherWatching {
		return nil, errPlacementWatcherIsNotWatching
	}
	if value == nil {
		return nil, errNilValue
	}
	t.proto.Reset()
	if err := value.Unmarshal(t.proto); err != nil {
		return nil, err
	}
	version := value.Version()
	return NewStagedPlacement(version, t.proto, t.placementOpts)
}

func (t *stagedPlacementWatcher) process(value interface{}) error {
	t.Lock()
	defer t.Unlock()

	if t.state != placementWatcherWatching {
		return errPlacementWatcherIsNotWatching
	}
	ps := value.(services.StagedPlacement)
	placement := ps.ActiveStagedPlacement(t.nowFn().UnixNano())
	if t.placement != nil {
		t.placement.Close()
	}
	t.placement = placement
	return nil
}
