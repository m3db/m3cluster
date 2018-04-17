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

package runtime

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3x/close"
	"github.com/m3db/m3x/log"
)

var (
	errInitWatchTimeout = errors.New("init watch timeout")
	errNilValue         = errors.New("nil kv value")
)

// Value is a value that can be updated during runtime.
type Value interface {
	// Watch starts watching for value updates.
	Watch() error

	// Unwatch stops watching for value updates.
	Unwatch()
}

// Updatable can be updated.
// TODO: Move to m3x/watch.
type Updatable interface {
	close.SimpleCloser

	// C returns the notification channel for updates.
	C() <-chan struct{}
}

// NewUpdatableFn creates an updatable.
type NewUpdatableFn func() (Updatable, error)

// GetFn returns the latest value.
type GetFn func(updatable Updatable) (kv.Versionable, error)

// ProcessFn processes a value.
// If the value is versionable, then only newer value will be processed.
type ProcessFn func(value kv.Versionable) error

// updateWithLockFn updates a value while holding a lock.
type updateWithLockFn func(value kv.Versionable) error

type valueStatus int

const (
	valueNotWatching valueStatus = iota
	valueWatching
)

type value struct {
	sync.RWMutex

	opts             Options
	log              log.Logger
	newUpdatableFn   NewUpdatableFn
	getFn            GetFn
	processFn        ProcessFn
	updateWithLockFn updateWithLockFn

	updatable Updatable
	status    valueStatus
	currValue kv.Versionable
}

// NewValue creates a new value.
func NewValue(
	opts Options,
) Value {
	v := &value{
		opts:           opts,
		log:            opts.InstrumentOptions().Logger(),
		newUpdatableFn: opts.NewUpdatableFn(),
		getFn:          opts.GetFn(),
		processFn:      opts.ProcessFn(),
	}
	v.updateWithLockFn = v.updateWithLock
	return v
}

func (v *value) Watch() error {
	v.Lock()
	defer v.Unlock()

	if v.status == valueWatching {
		return nil
	}
	updatable, err := v.newUpdatableFn()
	if err != nil {
		return CreateWatchError{innerError: err}
	}
	v.status = valueWatching
	v.updatable = updatable
	// NB(xichen): we want to start watching updates even though
	// we may fail to initialize the value temporarily (e.g., during
	// a network partition) so the value will be updated when the
	// error condition is resolved.
	defer func() { go v.watchUpdates(v.updatable) }()

	select {
	case <-v.updatable.C():
	case <-time.After(v.opts.InitWatchTimeout()):
		return InitValueError{innerError: errInitWatchTimeout}
	}

	update, err := v.getFn(v.updatable)
	if err != nil {
		return InitValueError{innerError: err}
	}

	if err = v.updateWithLockFn(update); err != nil {
		return InitValueError{innerError: err}
	}
	return nil
}

func (v *value) Unwatch() {
	v.Lock()
	defer v.Unlock()

	// If status is nil, it means we are not watching.
	if v.status == valueNotWatching {
		return
	}
	v.updatable.Close()
	v.status = valueNotWatching
	v.updatable = nil
}

func (v *value) watchUpdates(updatable Updatable) {
	for range updatable.C() {
		v.Lock()
		// If we are not watching, or we are watching with a different
		// watch because we stopped the current watch and started a new
		// one, return immediately.
		if v.status != valueWatching || v.updatable != updatable {
			v.Unlock()
			return
		}
		update, err := v.getFn(updatable)
		if err != nil {
			v.log.Errorf("error getting update: %v", err)
			v.Unlock()
			continue
		}
		if err = v.updateWithLockFn(update); err != nil {
			v.log.Errorf("error updating value: %v", err)
		}
		v.Unlock()
	}
}

func (v *value) updateWithLock(update kv.Versionable) error {
	if update == nil {
		return errNilValue
	}
	if !v.isNewerUpdate(update) {
		return nil
	}
	if err := v.processFn(update); err != nil {
		return err
	}
	v.currValue = update
	return nil
}

func (v *value) isNewerUpdate(update kv.Versionable) bool {
	if v.currValue == nil {
		return true
	}
	if update.IsNewer(v.currValue) {
		return true
	}
	v.log.Warnf(
		"ignore update with version %d which is not newer than the version of the current value: %d",
		update.Version(), v.currValue.Version(),
	)
	return false
}

// CreateWatchError is returned when encountering an error creating a watch.
type CreateWatchError struct {
	innerError error
}

func (e CreateWatchError) Error() string {
	return fmt.Sprintf("create watch error:%v", e.innerError)
}

// InitValueError is returned when encountering an error when initializing a value.
type InitValueError struct {
	innerError error
}

func (e InitValueError) Error() string {
	return fmt.Sprintf("initializing value error:%v", e.innerError)
}
