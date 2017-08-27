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

package util

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3cluster/generated/proto/commonpb"
	"github.com/m3db/m3cluster/kv/mem"
	"github.com/stretchr/testify/require"
)

func TestWatchAndUpdateBool(t *testing.T) {
	testConfig := struct {
		sync.RWMutex
		v bool
	}{}

	valueFn := func() bool {
		testConfig.RLock()
		defer testConfig.RUnlock()

		return testConfig.v
	}

	store := mem.NewStore()

	w, err := WatchAndUpdateBool(store, "foo", &testConfig.v, &testConfig.RWMutex, true, nil, nil)
	require.NoError(t, err)

	_, err = store.Set("foo", &commonpb.BoolProto{Value: true})
	require.NoError(t, err)
	for {
		if valueFn() {
			break
		}
	}

	_, err = store.Set("foo", &commonpb.BoolProto{Value: false})
	require.NoError(t, err)
	for {
		if !valueFn() {
			break
		}
	}

	_, err = store.Set("foo", &commonpb.Float64Proto{Value: 20})
	require.NoError(t, err)
	for {
		if valueFn() {
			break
		}
	}

	_, err = store.Set("foo", &commonpb.BoolProto{Value: false})
	require.NoError(t, err)
	for {
		if !valueFn() {
			break
		}
	}

	_, err = store.Delete("foo")
	require.NoError(t, err)
	for {
		if valueFn() {
			break
		}
	}

	w.Close()

	_, err = store.Set("foo", &commonpb.BoolProto{Value: false})
	require.NoError(t, err)

	// No longer receives update.
	time.Sleep(100 * time.Millisecond)
	require.True(t, valueFn())
}

func TestWatchAndUpdateFloat64(t *testing.T) {
	testConfig := struct {
		sync.RWMutex
		v float64
	}{}

	valueFn := func() float64 {
		testConfig.RLock()
		defer testConfig.RUnlock()

		return testConfig.v
	}

	store := mem.NewStore()

	w, err := WatchAndUpdateFloat64(store, "foo", &testConfig.v, &testConfig.RWMutex, 12.3, nil, nil)
	require.NoError(t, err)

	_, err = store.Set("foo", &commonpb.Int64Proto{Value: 1})
	require.NoError(t, err)
	for {
		if valueFn() == 12.3 {
			break
		}
	}

	_, err = store.Set("foo", &commonpb.Float64Proto{Value: 1.2})
	require.NoError(t, err)
	for {
		if valueFn() == 1.2 {
			break
		}
	}

	_, err = store.Delete("foo")
	require.NoError(t, err)
	for {
		if valueFn() == 12.3 {
			break
		}
	}

	w.Close()

	_, err = store.Set("foo", &commonpb.Float64Proto{Value: 1.2})
	require.NoError(t, err)

	// No longer receives update.
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, 12.3, valueFn())
}

func TestWatchAndUpdateInt64(t *testing.T) {
	testConfig := struct {
		sync.RWMutex
		v int64
	}{}

	valueFn := func() int64 {
		testConfig.RLock()
		defer testConfig.RUnlock()

		return testConfig.v
	}

	store := mem.NewStore()

	w, err := WatchAndUpdateInt64(store, "foo", &testConfig.v, &testConfig.RWMutex, 12, nil, nil)
	require.NoError(t, err)

	_, err = store.Set("foo", &commonpb.Float64Proto{Value: 100})
	require.NoError(t, err)
	for {
		if valueFn() == 12 {
			break
		}
	}

	_, err = store.Set("foo", &commonpb.Int64Proto{Value: 1})
	require.NoError(t, err)
	for {
		if valueFn() == 1 {
			break
		}
	}

	_, err = store.Delete("foo")
	require.NoError(t, err)
	for {
		if valueFn() == 12 {
			break
		}
	}

	w.Close()

	_, err = store.Set("foo", &commonpb.Int64Proto{Value: 1})
	require.NoError(t, err)

	// No longer receives update.
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, int64(12), valueFn())
}

func TestWatchAndUpdateString(t *testing.T) {
	testConfig := struct {
		sync.RWMutex
		v string
	}{}

	valueFn := func() string {
		testConfig.RLock()
		defer testConfig.RUnlock()

		return testConfig.v
	}

	store := mem.NewStore()

	w, err := WatchAndUpdateString(store, "foo", &testConfig.v, &testConfig.RWMutex, "bar", nil, nil)
	require.NoError(t, err)

	_, err = store.Set("foo", &commonpb.Float64Proto{Value: 100})
	require.NoError(t, err)
	for {
		if valueFn() == "bar" {
			break
		}
	}

	_, err = store.Set("foo", &commonpb.StringProto{Value: "baz"})
	require.NoError(t, err)
	for {
		if valueFn() == "baz" {
			break
		}
	}

	_, err = store.Delete("foo")
	require.NoError(t, err)
	for {
		if valueFn() == "bar" {
			break
		}
	}

	w.Close()

	_, err = store.Set("foo", &commonpb.StringProto{Value: "lol"})
	require.NoError(t, err)

	// No longer receives update.
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, "bar", valueFn())
}

func TestWatchAndUpdateTime(t *testing.T) {
	testConfig := struct {
		sync.RWMutex
		v time.Time
	}{}

	valueFn := func() time.Time {
		testConfig.RLock()
		defer testConfig.RUnlock()

		return testConfig.v
	}

	var (
		store       = mem.NewStore()
		now         = time.Now()
		defaultTime = now.Add(time.Hour)
	)

	w, err := WatchAndUpdateTime(store, "foo", &testConfig.v, &testConfig.RWMutex, defaultTime, nil, nil)
	require.NoError(t, err)

	_, err = store.Set("foo", &commonpb.Float64Proto{Value: 100})
	require.NoError(t, err)
	for {
		if valueFn() == defaultTime {
			break
		}
	}

	_, err = store.Set("foo", &commonpb.Int64Proto{Value: now.Unix()})
	require.NoError(t, err)
	for {
		if valueFn().Unix() == now.Unix() {
			break
		}
	}

	_, err = store.Delete("foo")
	require.NoError(t, err)
	for {
		if valueFn() == defaultTime {
			break
		}
	}

	w.Close()

	_, err = store.Set("foo", &commonpb.Int64Proto{Value: now.Unix()})
	require.NoError(t, err)

	// No longer receives update.
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, defaultTime, valueFn())
}

func TestWatchAndUpdateWithValidationBool(t *testing.T) {
	testConfig := struct {
		sync.RWMutex
		v bool
	}{}

	valueFn := func() bool {
		testConfig.RLock()
		defer testConfig.RUnlock()

		return testConfig.v
	}

	validateFn := func(val interface{}) error {
		v, ok := val.(bool)
		if !ok {
			return fmt.Errorf("invalid type for val, expected boll, received %T", val)
		}

		if !v {
			return errors.New("value of update is false, must be true")
		}

		return nil
	}

	store := mem.NewStore()

	w, err := WatchAndUpdateBool(store, "foo", &testConfig.v, &testConfig.RWMutex, true, validateFn, nil)
	require.NoError(t, err)

	_, err = store.Set("foo", &commonpb.BoolProto{Value: true})
	require.NoError(t, err)
	for {
		if valueFn() {
			break
		}
	}

	// Invalid update.
	_, err = store.Set("foo", &commonpb.BoolProto{Value: false})
	require.NoError(t, err)
	for {
		if valueFn() {
			break
		}
	}

	_, err = store.Set("foo", &commonpb.Float64Proto{Value: 20})
	require.NoError(t, err)
	for {
		if valueFn() {
			break
		}
	}

	_, err = store.Delete("foo")
	require.NoError(t, err)
	for {
		if valueFn() {
			break
		}
	}

	w.Close()

	require.NoError(t, err)
}

func TestWatchAndUpdateWithValidationFloat64(t *testing.T) {
	testConfig := struct {
		sync.RWMutex
		v float64
	}{}

	valueFn := func() float64 {
		testConfig.RLock()
		defer testConfig.RUnlock()

		return testConfig.v
	}

	validateFn := func(val interface{}) error {
		v, ok := val.(float64)
		if !ok {
			return fmt.Errorf("invalid type for val, expected float64, received %T", val)
		}

		if v > 20 {
			return fmt.Errorf("val must be < 20, is %v", v)
		}

		return nil
	}

	store := mem.NewStore()

	w, err := WatchAndUpdateFloat64(
		store, "foo", &testConfig.v, &testConfig.RWMutex, 15, validateFn, nil,
	)
	require.NoError(t, err)

	_, err = store.Set("foo", &commonpb.Float64Proto{Value: 17})
	require.NoError(t, err)
	for {
		if valueFn() == 17 {
			break
		}
	}

	// Invalid update.
	_, err = store.Set("foo", &commonpb.Float64Proto{Value: 22})
	require.NoError(t, err)
	for {
		if valueFn() == 17 {
			break
		}
	}

	_, err = store.Set("foo", &commonpb.Int64Proto{Value: 1})
	require.NoError(t, err)
	for {
		if valueFn() == 17 {
			break
		}
	}

	_, err = store.Delete("foo")
	require.NoError(t, err)
	for {
		if valueFn() == 15 {
			break
		}
	}

	w.Close()

	_, err = store.Set("foo", &commonpb.Float64Proto{Value: 13})
	require.NoError(t, err)

	// No longer receives update.
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, float64(15), valueFn())
}

func TestWatchAndUpdateWithValidationInt64(t *testing.T) {
	testConfig := struct {
		sync.RWMutex
		v int64
	}{}

	valueFn := func() int64 {
		testConfig.RLock()
		defer testConfig.RUnlock()

		return testConfig.v
	}

	validateFn := func(val interface{}) error {
		v, ok := val.(int64)
		if !ok {
			return fmt.Errorf("invalid type for val, expected int64, received %T", val)
		}

		if v > 20 {
			return fmt.Errorf("val must be < 20, is %v", v)
		}

		return nil
	}

	store := mem.NewStore()

	w, err := WatchAndUpdateInt64(
		store, "foo", &testConfig.v, &testConfig.RWMutex, 15, validateFn, nil,
	)
	require.NoError(t, err)

	_, err = store.Set("foo", &commonpb.Int64Proto{Value: 17})
	require.NoError(t, err)
	for {
		if valueFn() == 17 {
			break
		}
	}

	// Invalid update.
	_, err = store.Set("foo", &commonpb.Int64Proto{Value: 22})
	require.NoError(t, err)
	for {
		if valueFn() == 17 {
			break
		}
	}

	_, err = store.Set("foo", &commonpb.Float64Proto{Value: 1})
	require.NoError(t, err)
	for {
		if valueFn() == 17 {
			break
		}
	}

	_, err = store.Delete("foo")
	require.NoError(t, err)
	for {
		if valueFn() == 15 {
			break
		}
	}

	w.Close()

	_, err = store.Set("foo", &commonpb.Int64Proto{Value: 13})
	require.NoError(t, err)

	// No longer receives update.
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, int64(15), valueFn())
}

func TestWatchAndUpdateWithValidationString(t *testing.T) {
	testConfig := struct {
		sync.RWMutex
		v string
	}{}

	valueFn := func() string {
		testConfig.RLock()
		defer testConfig.RUnlock()

		return testConfig.v
	}

	validateFn := func(val interface{}) error {
		v, ok := val.(string)
		if !ok {
			return fmt.Errorf("invalid type for val, expected string, received %T", val)
		}

		if !strings.HasPrefix(v, "b") {
			return fmt.Errorf("val must start with 'b', is %v", v)
		}

		return nil
	}

	store := mem.NewStore()

	w, err := WatchAndUpdateString(
		store, "foo", &testConfig.v, &testConfig.RWMutex, "bar", validateFn, nil,
	)
	require.NoError(t, err)

	_, err = store.Set("foo", &commonpb.StringProto{Value: "baz"})
	require.NoError(t, err)
	for {
		if valueFn() == "baz" {
			break
		}
	}

	// Invalid update.
	_, err = store.Set("foo", &commonpb.StringProto{Value: "cat"})
	require.NoError(t, err)
	for {
		if valueFn() == "baz" {
			break
		}
	}

	_, err = store.Set("foo", &commonpb.Float64Proto{Value: 100})
	require.NoError(t, err)
	for {
		if valueFn() == "bar" {
			break
		}
	}

	_, err = store.Delete("foo")
	require.NoError(t, err)
	for {
		if valueFn() == "bar" {
			break
		}
	}

	w.Close()

	_, err = store.Set("foo", &commonpb.StringProto{Value: "lol"})
	require.NoError(t, err)

	// No longer receives update.
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, "bar", valueFn())
}

func TestWatchAndUpdateWithValidationTime(t *testing.T) {
	testConfig := struct {
		sync.RWMutex
		v time.Time
	}{}

	valueFn := func() time.Time {
		testConfig.RLock()
		defer testConfig.RUnlock()

		return testConfig.v
	}

	var (
		store       = mem.NewStore()
		defaultTime = time.Now()
	)

	validateFn := func(val interface{}) error {
		v, ok := val.(time.Time)
		if !ok {
			return fmt.Errorf("invalid type for val, expected time.Time, received %T", val)
		}

		bound := defaultTime.Add(time.Minute)
		if v.After(bound) {
			return fmt.Errorf("val must be before %v, is %v", bound, v)
		}

		return nil
	}

	w, err := WatchAndUpdateTime(
		store, "foo", &testConfig.v, &testConfig.RWMutex, defaultTime, validateFn, nil,
	)
	require.NoError(t, err)

	newTime := defaultTime.Add(30 * time.Second)

	fmt.Println(defaultTime)
	fmt.Println(newTime)

	_, err = store.Set("foo", &commonpb.Int64Proto{Value: newTime.Unix()})
	require.NoError(t, err)
	for {
		if valueFn().Unix() == newTime.Unix() {
			break
		}
	}

	// Invalid update.
	invalidTime := defaultTime.Add(2 * time.Minute)
	_, err = store.Set("foo", &commonpb.Int64Proto{Value: invalidTime.Unix()})
	require.NoError(t, err)
	for {
		if valueFn().Unix() == newTime.Unix() {
			break
		}
	}

	_, err = store.Set("foo", &commonpb.Float64Proto{Value: 1})
	require.NoError(t, err)
	for {
		if valueFn().Unix() == newTime.Unix() {
			break
		}
	}

	_, err = store.Delete("foo")
	require.NoError(t, err)
	for {
		if valueFn().Unix() == defaultTime.Unix() {
			break
		}
	}

	w.Close()

	_, err = store.Set("foo", &commonpb.Int64Proto{Value: newTime.Unix()})
	require.NoError(t, err)

	// No longer receives update.
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, defaultTime, valueFn())
}

func TestBoolFromValue(t *testing.T) {
	require.True(t, BoolFromValue(mem.NewValue(0, &commonpb.BoolProto{Value: true}), "key", false, nil))
	require.False(t, BoolFromValue(mem.NewValue(0, &commonpb.BoolProto{Value: false}), "key", true, nil))

	require.True(t, BoolFromValue(mem.NewValue(0, &commonpb.Float64Proto{Value: 123}), "key", true, nil))
	require.False(t, BoolFromValue(mem.NewValue(0, &commonpb.Float64Proto{Value: 123}), "key", false, nil))

	require.True(t, BoolFromValue(nil, "key", true, nil))
	require.False(t, BoolFromValue(nil, "key", false, nil))
}

func TestFloat64FromValue(t *testing.T) {
	require.Equal(t, 20.5, Float64FromValue(mem.NewValue(0, &commonpb.Int64Proto{Value: 200}), "key", 20.5, nil))
	require.Equal(t, 123.3, Float64FromValue(mem.NewValue(0, &commonpb.Float64Proto{Value: 123.3}), "key", 20, nil))
	require.Equal(t, 20.1, Float64FromValue(nil, "key", 20.1, nil))
}

func TestInt64FromValue(t *testing.T) {
	require.Equal(t, int64(200), Int64FromValue(mem.NewValue(0, &commonpb.Int64Proto{Value: 200}), "key", 20, nil))
	require.Equal(t, int64(20), Int64FromValue(mem.NewValue(0, &commonpb.Float64Proto{Value: 123}), "key", 20, nil))
	require.Equal(t, int64(20), Int64FromValue(nil, "key", 20, nil))
}

func TestStringArrayFromValue(t *testing.T) {
	defaultValue := []string{"d1", "d2"}
	v1 := []string{"s1", "s2"}

	require.Equal(t, v1, StringArrayFromValue(mem.NewValue(0, &commonpb.StringArrayProto{Values: v1}), "key", defaultValue, nil))
	require.Equal(t, defaultValue, StringArrayFromValue(mem.NewValue(0, &commonpb.Float64Proto{Value: 123}), "key", defaultValue, nil))
	require.Equal(t, defaultValue, StringArrayFromValue(nil, "key", defaultValue, nil))
}
