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
	"io/ioutil"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/integration"
	"github.com/golang/protobuf/proto"
	"github.com/m3db/m3cluster/generated/proto/kvtest"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetAndSet(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()

	store, err := NewStore(ec, opts)
	require.NoError(t, err)

	value, err := store.Get("foo")
	require.Error(t, err)
	require.Equal(t, kv.ErrNotFound, err)
	require.Nil(t, value)

	version, err := store.Set("foo", genProto("bar1"))
	require.NoError(t, err)
	require.Equal(t, 1, version)

	value, err = store.Get("foo")
	require.NoError(t, err)
	verifyValue(t, value, "bar1", 1)

	version, err = store.Set("foo", genProto("bar2"))
	require.NoError(t, err)
	require.Equal(t, 2, version)

	value, err = store.Get("foo")
	require.NoError(t, err)
	verifyValue(t, value, "bar2", 2)
}

func TestNoCache(t *testing.T) {
	ec, opts, closeFn := testStore(t)

	store, err := NewStore(ec, opts)
	require.NoError(t, err)
	require.Equal(t, 0, len(store.(*client).cacheUpdatedCh))

	version, err := store.Set("foo", genProto("bar1"))
	require.NoError(t, err)
	require.Equal(t, 1, version)

	value, err := store.Get("foo")
	require.NoError(t, err)
	verifyValue(t, value, "bar1", 1)
	// the will send a notification but won't trigger a sync
	// because no cache file is set
	require.Equal(t, 1, len(store.(*client).cacheUpdatedCh))

	closeFn()

	// from cache
	value, err = store.Get("foo")
	require.NoError(t, err)
	verifyValue(t, value, "bar1", 1)

	// new store but no cache file set
	store, err = NewStore(ec, opts)
	require.NoError(t, err)

	_, err = store.Set("foo", genProto("bar1"))
	require.Error(t, err)

	_, err = store.Get("foo")
	require.Error(t, err)
	require.Equal(t, 0, len(store.(*client).cacheUpdatedCh))
}

func TestCache(t *testing.T) {
	ec, opts, closeFn := testStore(t)

	f, err := ioutil.TempFile("", "")
	require.NoError(t, err)

	opts = opts.SetCacheFilePath(f.Name())

	store, err := NewStore(ec, opts)
	require.NoError(t, err)
	require.Equal(t, 0, len(store.(*client).cacheUpdatedCh))

	version, err := store.Set("foo", genProto("bar1"))
	require.NoError(t, err)
	require.Equal(t, 1, version)

	value, err := store.Get("foo")
	require.NoError(t, err)
	verifyValue(t, value, "bar1", 1)
	for {
		// the notification should be picked up and trigger a sync
		if len(store.(*client).cacheUpdatedCh) == 0 {
			break
		}
	}
	closeFn()

	// from cache
	value, err = store.Get("foo")
	require.NoError(t, err)
	verifyValue(t, value, "bar1", 1)
	require.Equal(t, 0, len(store.(*client).cacheUpdatedCh))

	// new store but with cache file
	store, err = NewStore(ec, opts)
	require.NoError(t, err)

	_, err = store.Set("key", genProto("bar1"))
	require.Error(t, err)

	_, err = store.Get("key")
	require.Error(t, err)
	require.Equal(t, 0, len(store.(*client).cacheUpdatedCh))

	value, err = store.Get("foo")
	require.NoError(t, err)
	verifyValue(t, value, "bar1", 1)
	require.Equal(t, 0, len(store.(*client).cacheUpdatedCh))
}

func TestSetIfNotExist(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()

	store, err := NewStore(ec, opts)
	require.NoError(t, err)

	version, err := store.SetIfNotExists("foo", genProto("bar"))
	require.NoError(t, err)
	require.Equal(t, 1, version)

	version, err = store.SetIfNotExists("foo", genProto("bar"))
	require.Error(t, err)
	require.Equal(t, kv.ErrAlreadyExists, err)

	value, err := store.Get("foo")
	require.NoError(t, err)
	verifyValue(t, value, "bar", 1)
}

func TestCheckAndSet(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()

	store, err := NewStore(ec, opts)
	require.NoError(t, err)

	version, err := store.CheckAndSet("foo", 1, genProto("bar"))
	require.Error(t, err)
	require.Equal(t, kv.ErrVersionMismatch, err)

	version, err = store.SetIfNotExists("foo", genProto("bar"))
	require.NoError(t, err)
	require.Equal(t, 1, version)

	version, err = store.CheckAndSet("foo", 1, genProto("bar"))
	require.NoError(t, err)
	require.Equal(t, 2, version)

	version, err = store.CheckAndSet("foo", 1, genProto("bar"))
	require.Error(t, err)
	require.Equal(t, kv.ErrVersionMismatch, err)

	value, err := store.Get("foo")
	require.NoError(t, err)
	verifyValue(t, value, "bar", 2)
}

func TestWatchClose(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()

	store, err := NewStore(ec, opts)
	require.NoError(t, err)

	_, err = store.Set("foo", genProto("bar1"))
	require.NoError(t, err)
	w1, err := store.Watch("foo")
	require.NoError(t, err)
	<-w1.C()
	verifyValue(t, w1.Get(), "bar1", 1)

	c := store.(*client)
	_, ok := c.watchables["test/foo"]
	require.True(t, ok)

	// closing w1 will close the go routine for the watch updates
	w1.Close()

	// waits until the original watchable is cleaned up
	for {
		c.RLock()
		_, ok = c.watchables["test/foo"]
		c.RUnlock()
		if !ok {
			break
		}
	}

	// getting a new watch will create a new watchale and thread to watch for updates
	w2, err := store.Watch("foo")
	require.NoError(t, err)
	<-w2.C()
	verifyValue(t, w2.Get(), "bar1", 1)

	// verify that w1 will no longer be updated because the original watchable is closed
	_, err = store.Set("foo", genProto("bar2"))
	require.NoError(t, err)
	<-w2.C()
	verifyValue(t, w2.Get(), "bar2", 2)
	verifyValue(t, w1.Get(), "bar1", 1)

	w1.Close()
	w2.Close()
}

func TestWatchLastVersion(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()

	store, err := NewStore(ec, opts)
	require.NoError(t, err)

	w, err := store.Watch("foo")
	require.NoError(t, err)
	require.Nil(t, w.Get())

	lastVersion := 100
	go func() {
		for i := 1; i <= lastVersion; i++ {
			_, err := store.Set("foo", genProto(fmt.Sprintf("bar%d", i)))
			require.NoError(t, err)
		}
	}()

	for {
		<-w.C()
		value := w.Get()
		if value.Version() == lastVersion {
			break
		}
	}
	verifyValue(t, w.Get(), fmt.Sprintf("bar%d", lastVersion), lastVersion)

	w.Close()
}

func TestWatchFromExist(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()

	store, err := NewStore(ec, opts)
	require.NoError(t, err)

	_, err = store.Set("foo", genProto("bar1"))
	require.NoError(t, err)
	value, err := store.Get("foo")
	require.NoError(t, err)
	verifyValue(t, value, "bar1", 1)

	w, err := store.Watch("foo")
	assert.NoError(t, err)

	<-w.C()
	require.Equal(t, 0, len(w.C()))
	verifyValue(t, w.Get(), "bar1", 1)

	_, err = store.Set("foo", genProto("bar2"))
	require.NoError(t, err)

	<-w.C()
	require.Equal(t, 0, len(w.C()))
	verifyValue(t, w.Get(), "bar2", 2)

	_, err = store.Set("foo", genProto("bar3"))
	require.NoError(t, err)

	<-w.C()
	require.Equal(t, 0, len(w.C()))
	verifyValue(t, w.Get(), "bar3", 3)

	w.Close()
}

func TestWatchFromNotExist(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()

	store, err := NewStore(ec, opts)
	require.NoError(t, err)

	w, err := store.Watch("foo")
	require.NoError(t, err)
	require.Equal(t, 0, len(w.C()))
	require.Nil(t, w.Get())

	_, err = store.Set("foo", genProto("bar1"))
	require.NoError(t, err)

	<-w.C()
	require.Equal(t, 0, len(w.C()))
	verifyValue(t, w.Get(), "bar1", 1)

	_, err = store.Set("foo", genProto("bar2"))
	require.NoError(t, err)

	<-w.C()
	require.Equal(t, 0, len(w.C()))
	verifyValue(t, w.Get(), "bar2", 2)

	w.Close()
}

func TestMultipleWatchesFromExist(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()

	store, err := NewStore(ec, opts)
	require.NoError(t, err)

	_, err = store.Set("foo", genProto("bar1"))
	require.NoError(t, err)

	w1, err := store.Watch("foo")
	require.NoError(t, err)

	w2, err := store.Watch("foo")
	require.NoError(t, err)

	<-w1.C()
	require.Equal(t, 0, len(w1.C()))
	verifyValue(t, w1.Get(), "bar1", 1)

	<-w2.C()
	require.Equal(t, 0, len(w2.C()))
	verifyValue(t, w2.Get(), "bar1", 1)

	_, err = store.Set("foo", genProto("bar2"))
	require.NoError(t, err)

	<-w1.C()
	require.Equal(t, 0, len(w1.C()))
	verifyValue(t, w1.Get(), "bar2", 2)

	<-w2.C()
	require.Equal(t, 0, len(w2.C()))
	verifyValue(t, w2.Get(), "bar2", 2)

	_, err = store.Set("foo", genProto("bar3"))
	require.NoError(t, err)

	<-w1.C()
	require.Equal(t, 0, len(w1.C()))
	verifyValue(t, w1.Get(), "bar3", 3)

	<-w2.C()
	require.Equal(t, 0, len(w2.C()))
	verifyValue(t, w2.Get(), "bar3", 3)

	w1.Close()
	w2.Close()
}

func TestMultipleWatchesFromNotExist(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()

	store, err := NewStore(ec, opts)
	require.NoError(t, err)
	w1, err := store.Watch("foo")
	require.NoError(t, err)
	require.Equal(t, 0, len(w1.C()))
	require.Nil(t, w1.Get())

	w2, err := store.Watch("foo")
	require.NoError(t, err)
	require.Equal(t, 0, len(w2.C()))
	require.Nil(t, w2.Get())

	_, err = store.Set("foo", genProto("bar1"))
	require.NoError(t, err)

	<-w1.C()
	require.Equal(t, 0, len(w1.C()))
	verifyValue(t, w1.Get(), "bar1", 1)

	<-w2.C()
	require.Equal(t, 0, len(w2.C()))
	verifyValue(t, w2.Get(), "bar1", 1)

	_, err = store.Set("foo", genProto("bar2"))
	require.NoError(t, err)

	<-w1.C()
	require.Equal(t, 0, len(w1.C()))
	verifyValue(t, w1.Get(), "bar2", 2)

	<-w2.C()
	require.Equal(t, 0, len(w2.C()))
	verifyValue(t, w2.Get(), "bar2", 2)

	w1.Close()
	w2.Close()
}

func TestWatchNonBlocking(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()

	opts = opts.SetWatchChanResetInterval(200 * time.Millisecond).SetWatchChanInitTimeout(200 * time.Millisecond)

	store, err := NewStore(ec, opts)
	require.NoError(t, err)
	c := store.(*client)

	failTotal := 3
	mw := mocks.NewBlackholeWatcher(ec, failTotal, func() { time.Sleep(time.Minute) })
	c.watcher = mw

	_, err = c.Set("foo", genProto("bar1"))
	require.NoError(t, err)

	before := time.Now()
	w1, err := c.Watch("foo")
	require.WithinDuration(t, time.Now(), before, 100*time.Millisecond)
	require.NoError(t, err)
	require.Equal(t, 0, len(w1.C()))

	// watch channel will error out, but Get() will be tried
	<-w1.C()
	verifyValue(t, w1.Get(), "bar1", 1)

	w1.Close()
}

func verifyValue(t *testing.T, v kv.Value, value string, version int) {
	var testMsg kvtest.Foo
	err := v.Unmarshal(&testMsg)
	require.NoError(t, err)
	require.Equal(t, value, testMsg.Msg)
	require.Equal(t, version, v.Version())
}

func genProto(msg string) proto.Message {
	return &kvtest.Foo{Msg: msg}
}

func testStore(t *testing.T) (*clientv3.Client, Options, func()) {
	ecluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	ec := ecluster.RandClient()

	closer := func() {
		ecluster.Terminate(t)
	}

	opts := NewOptions().
		SetWatchChanCheckInterval(10 * time.Millisecond).
		SetKeyFn(func(key string) string {
			return fmt.Sprintf("test/%s", key)
		})

	return ec, opts, closer
}

func TestHistory(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()

	store, err := NewStore(ec, opts)
	res, err := store.History("k1", 10, 5)
	assert.Error(t, err)

	res, err = store.History("k1", 0, 5)
	assert.Error(t, err)

	res, err = store.History("k1", -5, 0)
	assert.Error(t, err)

	totalVersion := 10
	for i := 1; i <= totalVersion; i++ {
		store.Set("k1", genProto(fmt.Sprintf("bar%d", i)))
		store.Set("k2", genProto(fmt.Sprintf("bar%d", i)))
	}

	res, err = store.History("k1", 6, 10)
	assert.NoError(t, err)
	assert.Equal(t, 4, len(res))
	for i := 0; i < len(res); i++ {
		version := i + 6
		value := res[i]
		verifyValue(t, value, fmt.Sprintf("bar%d", version), version)
	}

	res, err = store.History("k1", 3, 7)
	assert.NoError(t, err)
	assert.Equal(t, 4, len(res))
	for i := 0; i < len(res); i++ {
		version := i + 3
		value := res[i]
		verifyValue(t, value, fmt.Sprintf("bar%d", version), version)
	}

	res, err = store.History("k1", 5, 15)
	assert.NoError(t, err)
	assert.Equal(t, totalVersion-5+1, len(res))
	for i := 0; i < len(res); i++ {
		version := i + 5
		value := res[i]
		verifyValue(t, value, fmt.Sprintf("bar%d", version), version)
	}

}
