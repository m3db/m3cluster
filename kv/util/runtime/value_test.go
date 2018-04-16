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
	"sync/atomic"
	"testing"
	"time"

	"github.com/m3db/m3cluster/generated/proto/commonpb"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/kv/mem"
	"github.com/m3db/m3x/instrument"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

const (
	testValueKey = "testValue"
)

func TestValueWatchAlreadyWatching(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockWatch := kv.NewMockValueWatch(ctrl)
	rv := NewValue(testValueOptions().SetUpdatableFn(testUpdatableFn(mockWatch))).(*value)
	rv.status = valueWatching
	require.NoError(t, rv.Watch())
}

func TestValueWatchCreateWatchError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errWatch := errors.New("error creating watch")
	updatableFn := func() (Updatable, error) {
		return nil, errWatch
	}
	rv := NewValue(testValueOptions().SetUpdatableFn(updatableFn)).(*value)

	err := rv.Watch()
	require.Equal(t, CreateWatchError{innerError: errWatch}, err)
	require.Equal(t, valueNotWatching, rv.status)

	rv.Unwatch()
	require.Equal(t, valueNotWatching, rv.status)
}

func TestValueWatchWatchTimeout(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	notifyCh := make(chan struct{})
	mockWatch := kv.NewMockValueWatch(ctrl)
	mockWatch.EXPECT().C().Return(notifyCh).MinTimes(1)
	mockWatch.EXPECT().Close().Do(func() { close(notifyCh) })
	rv := NewValue(testValueOptions().SetUpdatableFn(testUpdatableFn(mockWatch))).(*value)

	err := rv.Watch()
	require.Equal(t, InitValueError{innerError: errInitWatchTimeout}, err)
	require.Equal(t, valueWatching, rv.status)

	rv.Unwatch()
	require.Equal(t, valueNotWatching, rv.status)
}

func TestValueWatchUpdateError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errUpdate := errors.New("error updating")
	notifyCh := make(chan struct{}, 1)
	notifyCh <- struct{}{}
	mockWatch := kv.NewMockValueWatch(ctrl)
	mockWatch.EXPECT().C().Return(notifyCh).MinTimes(1)
	mockWatch.EXPECT().Close().Do(func() { close(notifyCh) })

	opts := testValueOptions().
		SetUpdatableFn(testUpdatableFn(mockWatch)).
		SetGetFn(func(Updatable) (interface{}, error) { return nil, nil })
	rv := NewValue(opts).(*value)
	rv.updateWithLockFn = func(interface{}) error { return errUpdate }

	require.Equal(t, InitValueError{innerError: errUpdate}, rv.Watch())
	require.Equal(t, valueWatching, rv.status)

	rv.Unwatch()
	require.Equal(t, valueNotWatching, rv.status)
}

func TestValueWatchSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	notifyCh := make(chan struct{}, 1)
	notifyCh <- struct{}{}
	mockWatch := kv.NewMockValueWatch(ctrl)
	mockWatch.EXPECT().C().Return(notifyCh).MinTimes(1)
	mockWatch.EXPECT().Close().Do(func() { close(notifyCh) })

	opts := testValueOptions().
		SetUpdatableFn(testUpdatableFn(mockWatch)).
		SetGetFn(func(Updatable) (interface{}, error) { return nil, nil })
	rv := NewValue(opts).(*value)
	rv.updateWithLockFn = func(interface{}) error { return nil }

	require.NoError(t, rv.Watch())
	require.Equal(t, valueWatching, rv.status)

	rv.Unwatch()
	require.Equal(t, valueNotWatching, rv.status)
}

func TestValueUnwatchNotWatching(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rv := NewValue(testValueOptions()).(*value)
	rv.status = valueNotWatching
	rv.Unwatch()
	require.Equal(t, valueNotWatching, rv.status)
}

func TestValueWatchUnWatchMultipleTimes(t *testing.T) {
	store, rv := testValueWithMemStore(t)
	rv.updateWithLockFn = func(interface{}) error { return nil }
	_, err := store.SetIfNotExists(testValueKey, &commonpb.BoolProto{})
	require.NoError(t, err)

	iter := 10
	for i := 0; i < iter; i++ {
		require.NoError(t, rv.Watch())
		rv.Unwatch()
	}
}

func TestValueWatchUpdatesError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	watchCh := make(chan struct{})
	doneCh := make(chan struct{})
	rv := NewValue(testValueOptions()).(*value)
	errUpdate := errors.New("error updating")
	rv.updateWithLockFn = func(interface{}) error {
		close(doneCh)
		return errUpdate
	}
	mockWatch := kv.NewMockValueWatch(ctrl)
	mockWatch.EXPECT().C().Return(watchCh).AnyTimes()
	mockWatch.EXPECT().Close().Do(func() { close(watchCh) })
	rv.updatable = mockWatch
	rv.getFn = func(Updatable) (interface{}, error) { return nil, nil }
	rv.status = valueWatching
	go rv.watchUpdates(mockWatch)

	watchCh <- struct{}{}
	<-doneCh
	rv.Unwatch()
	require.Equal(t, valueNotWatching, rv.status)
}

func TestValueWatchValueUnwatched(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rv := NewValue(testValueOptions()).(*value)
	var updated int32
	rv.updateWithLockFn = func(interface{}) error { atomic.AddInt32(&updated, 1); return nil }
	ch := make(chan struct{})
	mockWatch := kv.NewMockValueWatch(ctrl)
	mockWatch.EXPECT().C().Return(ch).AnyTimes()
	mockWatch.EXPECT().Close().Do(func() { close(ch) }).AnyTimes()
	rv.updatable = mockWatch
	rv.status = valueNotWatching
	go rv.watchUpdates(mockWatch)

	ch <- struct{}{}
	// Given the update goroutine a chance to run.
	time.Sleep(100 * time.Millisecond)
	close(ch)
	require.Equal(t, int32(0), atomic.LoadInt32(&updated))
}

func TestValueWatchValueDifferentWatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rv := NewValue(testValueOptions()).(*value)
	var updated int32
	rv.updateWithLockFn = func(interface{}) error { atomic.AddInt32(&updated, 1); return nil }
	ch := make(chan struct{})
	mockWatch := kv.NewMockValueWatch(ctrl)
	mockWatch.EXPECT().C().Return(ch).AnyTimes()
	mockWatch.EXPECT().Close().Do(func() { close(ch) }).AnyTimes()
	rv.updatable = kv.NewMockValueWatch(ctrl)
	rv.status = valueWatching
	go rv.watchUpdates(mockWatch)

	ch <- struct{}{}
	// Given the update goroutine a chance to run.
	time.Sleep(100 * time.Millisecond)
	close(ch)
	require.Equal(t, int32(0), atomic.LoadInt32(&updated))
}

func TestValueUpdateNilValueError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rv := NewValue(testValueOptions()).(*value)
	require.Equal(t, errNilValue, rv.updateWithLockFn(nil))
}

func TestValueUpdateProcessError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rv := NewValue(testValueOptions()).(*value)
	errProcess := errors.New("error processing")
	rv.processFn = func(interface{}) error { return errProcess }

	require.Error(t, rv.updateWithLockFn(mem.NewValue(3, nil)))
}

func TestValueUpdateSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var outputs []kv.Value
	rv := NewValue(testValueOptions()).(*value)
	rv.processFn = func(v interface{}) error {
		outputs = append(outputs, v.(kv.Value))
		return nil
	}

	input := mem.NewValue(3, nil)
	require.NoError(t, rv.updateWithLock(input))
	require.Equal(t, []kv.Value{input}, outputs)
}

func testValueOptions() Options {
	return NewOptions().
		SetInstrumentOptions(instrument.NewOptions()).
		SetInitWatchTimeout(100 * time.Millisecond).
		SetProcessFn(nil)
}

func testValueWithMemStore(t *testing.T) (kv.Store, *value) {
	store := mem.NewStore()
	w, err := store.Watch(testValueKey)
	require.NoError(t, err)
	opts := testValueOptions().
		SetUpdatableFn(testUpdatableFn(w)).
		SetGetFn(func(value Updatable) (interface{}, error) {
			return value.(kv.ValueWatch).Get(), nil
		})
	return store, NewValue(opts).(*value)
}

func testUpdatableFn(n Updatable) NewUpdatableFn {
	return func() (Updatable, error) {
		return n, nil
	}
}
