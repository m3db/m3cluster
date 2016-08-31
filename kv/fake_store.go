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

package kv

import (
	"sync"

	"github.com/golang/protobuf/proto"
)

const (
	updateQueueSize = 10
)

// NewFakeStore returns a new in-process store that can be used for testing
func NewFakeStore() Store {
	return &fakeStore{
		values:  make(map[string]*fakeValue),
		updates: make(map[string][]chan map[string]Value),
	}
}

// NewFakeValue returns a new fake Value around the given proto
func NewFakeValue(vers int, msg proto.Message) Value {
	data, _ := proto.Marshal(msg)
	return &fakeValue{
		version: vers,
		data:    data,
	}
}

// NewFakeValueWithData returns a new fake Value around the given data
func NewFakeValueWithData(vers int, data []byte) Value {
	return &fakeValue{
		version: vers,
		data:    data,
	}
}

type fakeValue struct {
	version int
	data    []byte
}

func (v fakeValue) Version() int                      { return v.version }
func (v fakeValue) Unmarshal(msg proto.Message) error { return proto.Unmarshal(v.data, msg) }

type fakeStore struct {
	sync.RWMutex
	values  map[string]*fakeValue
	updates map[string][]chan map[string]Value
}

func (kv *fakeStore) Get(key string) (Value, error) {
	kv.RLock()
	defer kv.RUnlock()

	if val := kv.values[key]; val != nil {
		return val, nil
	}

	return nil, ErrNotFound
}

func (kv *fakeStore) Subscribe(keys []string) (Subscription, error) {
	if len(keys) == 0 {
		return nil, ErrNoKeysProvided
	}

	kv.Lock()
	defer kv.Unlock()
	for _, key := range keys {
		if _, ok := kv.values[key]; !ok {
			return nil, ErrNotFound
		}
	}

	ch := make(chan map[string]Value, updateQueueSize)
	initVals := make(map[string]Value)
	for _, key := range keys {
		kv.updates[key] = append(kv.updates[key], ch)
		initVals[key] = kv.values[key]
	}

	ch <- initVals
	return ch, nil
}

func (kv *fakeStore) Unsubscribe(s Subscription) {
	kv.Lock()
	defer kv.Unlock()

	var c chan map[string]Value
	for key, chs := range kv.updates {
		for i, ch := range chs {
			if s == ch {
				kv.updates[key] = append(chs[:i], chs[i+1:]...)
				c = ch
			}
		}
	}

	if c != nil {
		close(c)
	}
}

func (kv *fakeStore) Set(key string, val proto.Message) (int, error) {
	data, err := proto.Marshal(val)
	if err != nil {
		return 0, err
	}

	kv.Lock()
	defer kv.Unlock()

	lastVersion := 0
	if val := kv.values[key]; val != nil {
		lastVersion = val.version
	}

	newVersion := lastVersion + 1
	fv := &fakeValue{
		version: newVersion,
		data:    data,
	}
	kv.values[key] = fv
	kv.updateSubscriptions(key, fv)

	return newVersion, nil
}

func (kv *fakeStore) SetIfNotExists(key string, val proto.Message) (int, error) {
	data, err := proto.Marshal(val)
	if err != nil {
		return 0, err
	}

	kv.Lock()
	defer kv.Unlock()

	if _, exists := kv.values[key]; exists {
		return 0, ErrAlreadyExists
	}

	fv := &fakeValue{
		version: 1,
		data:    data,
	}
	kv.values[key] = fv
	kv.updateSubscriptions(key, fv)

	return 1, nil
}

func (kv *fakeStore) CheckAndSet(key string, version int, val proto.Message) (int, error) {
	data, err := proto.Marshal(val)
	if err != nil {
		return 0, err
	}

	kv.Lock()
	defer kv.Unlock()

	if val, exists := kv.values[key]; exists {
		if val.version != version {
			return 0, ErrVersionMismatch
		}
	}

	newVersion := version + 1
	fv := &fakeValue{
		version: newVersion,
		data:    data,
	}
	kv.values[key] = fv
	kv.updateSubscriptions(key, fv)

	return newVersion, nil
}

// updateSubscriptions updates all subscriptions for the given key. It assumes
// the fakeStore write lock is acquired outside of this call
func (kv *fakeStore) updateSubscriptions(key string, val Value) {
	if chs, ok := kv.updates[key]; ok {
		for _, ch := range chs {
			select {
			case ch <- map[string]Value{key: val}:
			default:
			}
		}
	}
}
