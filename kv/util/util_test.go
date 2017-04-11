package util

import (
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/proto"
	"github.com/m3db/m3cluster/generated/proto/commonpb"
	"github.com/m3db/m3cluster/kv"
	"github.com/stretchr/testify/require"
)

func TestBoolFromValue(t *testing.T) {
	require.True(t, BoolFromValue(newMockValue(t, &commonpb.BoolProto{Value: true}), "key", false))
	require.False(t, BoolFromValue(newMockValue(t, &commonpb.BoolProto{Value: false}), "key", true))

	require.True(t, BoolFromValue(newMockValue(t, &commonpb.Float64Proto{Value: 123}), "key", true))
	require.False(t, BoolFromValue(newMockValue(t, &commonpb.Float64Proto{Value: 123}), "key", false))

	require.True(t, BoolFromValue(nil, "key", true))
	require.False(t, BoolFromValue(nil, "key", false))
}

func TestUpdateBoolWithValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testConfig := struct {
		sync.RWMutex
		v bool
	}{}
	vw := kv.NewValueWatchable()
	_, w, _ := vw.Watch()

	vw.Update(newMockValue(t, &commonpb.BoolProto{Value: false}))
	<-w.C()
	UpdateBoolWithValue(w.Get(), "foo", &testConfig.v, &testConfig.RWMutex, true, nil)
	testConfig.Lock()
	require.False(t, testConfig.v)
	testConfig.Unlock()

	vw.Update(newMockValue(t, &commonpb.BoolProto{Value: true}))
	<-w.C()
	UpdateBoolWithValue(w.Get(), "foo", &testConfig.v, &testConfig.RWMutex, true, nil)
	testConfig.Lock()
	require.True(t, testConfig.v)
	testConfig.Unlock()

	// mimic a bad value update, should fall back to default
	vw.Update(newMockValue(t, &commonpb.Float64Proto{Value: 20}))
	<-w.C()
	UpdateBoolWithValue(w.Get(), "foo", &testConfig.v, &testConfig.RWMutex, true, nil)
	testConfig.Lock()
	require.True(t, testConfig.v)
	testConfig.Unlock()

	// mimic a delete on kv store, should fall back to default
	vw.Update(nil)
	<-w.C()
	UpdateBoolWithValue(w.Get(), "foo", &testConfig.v, &testConfig.RWMutex, true, nil)
	testConfig.Lock()
	require.True(t, testConfig.v)
	testConfig.Unlock()
}

func TestFloat64FromValue(t *testing.T) {
	require.Equal(t, 20.5, Float64FromValue(newMockValue(t, &commonpb.Int64Proto{Value: 200}), "key", 20.5))
	require.Equal(t, 123.3, Float64FromValue(newMockValue(t, &commonpb.Float64Proto{Value: 123.3}), "key", 20))
	require.Equal(t, 20.1, Float64FromValue(nil, "key", 20.1))
}

func TestUpdateFloat64WithValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testConfig := struct {
		sync.RWMutex
		v float64
	}{}
	vw := kv.NewValueWatchable()
	_, w, _ := vw.Watch()

	vw.Update(newMockValue(t, &commonpb.Float64Proto{Value: 20}))
	<-w.C()
	UpdateFloat64WithValue(w.Get(), "foo", &testConfig.v, &testConfig.RWMutex, 0, nil)
	testConfig.Lock()
	require.Equal(t, float64(20), testConfig.v)
	testConfig.Unlock()

	// mimic a bad value update, should fall back to default
	vw.Update(newMockValue(t, &commonpb.BoolProto{Value: true}))
	<-w.C()
	UpdateFloat64WithValue(w.Get(), "foo", &testConfig.v, &testConfig.RWMutex, 30, nil)
	testConfig.Lock()
	require.Equal(t, float64(30), testConfig.v)
	testConfig.Unlock()

	// mimic a delete on kv store, should fall back to default
	vw.Update(nil)
	<-w.C()
	UpdateFloat64WithValue(w.Get(), "foo", &testConfig.v, &testConfig.RWMutex, 40, nil)
	testConfig.Lock()
	require.Equal(t, float64(40), testConfig.v)
	testConfig.Unlock()
}

func TestInt64FromValue(t *testing.T) {
	require.Equal(t, int64(200), Int64FromValue(newMockValue(t, &commonpb.Int64Proto{Value: 200}), "key", 20))
	require.Equal(t, int64(20), Int64FromValue(newMockValue(t, &commonpb.Float64Proto{Value: 123}), "key", 20))
	require.Equal(t, int64(20), Int64FromValue(nil, "key", 20))
}

func TestUpdateInt64WithValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testConfig := struct {
		sync.RWMutex
		v int64
	}{}
	vw := kv.NewValueWatchable()
	_, w, _ := vw.Watch()

	vw.Update(newMockValue(t, &commonpb.Int64Proto{Value: 20}))
	<-w.C()
	UpdateInt64WithValue(w.Get(), "foo", &testConfig.v, &testConfig.RWMutex, 0, nil)
	testConfig.Lock()
	require.Equal(t, int64(20), testConfig.v)
	testConfig.Unlock()

	// mimic a bad value update, should fall back to default
	vw.Update(newMockValue(t, &commonpb.Float64Proto{Value: 100}))
	<-w.C()
	UpdateInt64WithValue(w.Get(), "foo", &testConfig.v, &testConfig.RWMutex, 30, nil)
	testConfig.Lock()
	require.Equal(t, int64(30), testConfig.v)
	testConfig.Unlock()

	// mimic a delete on kv store, should fall back to default
	vw.Update(nil)
	<-w.C()
	UpdateInt64WithValue(w.Get(), "foo", &testConfig.v, &testConfig.RWMutex, 40, nil)
	testConfig.Lock()
	require.Equal(t, int64(40), testConfig.v)
	testConfig.Unlock()
}

func TestStringArrayFromValue(t *testing.T) {
	defaultValue := []string{"d1", "d2"}
	v1 := []string{"s1", "s2"}

	require.Equal(t, v1, StringArrayFromValue(newMockValue(t, &commonpb.StringArrayProto{Values: v1}), "key", defaultValue))
	require.Equal(t, defaultValue, StringArrayFromValue(newMockValue(t, &commonpb.Float64Proto{Value: 123}), "key", defaultValue))
	require.Equal(t, defaultValue, StringArrayFromValue(nil, "key", defaultValue))
}

func TestUpdateStringArrayWithValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testConfig := struct {
		sync.RWMutex
		v []string
	}{}
	vw := kv.NewValueWatchable()
	_, w, _ := vw.Watch()

	defaultValue := []string{"d1", "d2"}

	v1 := []string{"s1", "s2"}
	vw.Update(newMockValue(t, &commonpb.StringArrayProto{Values: v1}))
	<-w.C()
	UpdateStringArrayWithValue(w.Get(), "foo", &testConfig.v, &testConfig.RWMutex, defaultValue, nil)
	testConfig.Lock()
	require.Equal(t, v1, testConfig.v)
	testConfig.Unlock()

	// mimic a bad value update, should fall back to default
	vw.Update(newMockValue(t, &commonpb.Float64Proto{Value: 100}))
	<-w.C()
	UpdateStringArrayWithValue(w.Get(), "foo", &testConfig.v, &testConfig.RWMutex, defaultValue, nil)
	testConfig.Lock()
	require.Equal(t, defaultValue, testConfig.v)
	testConfig.Unlock()

	// mimic a delete on kv store, should fall back to default
	vw.Update(nil)
	<-w.C()
	UpdateStringArrayWithValue(w.Get(), "foo", &testConfig.v, &testConfig.RWMutex, defaultValue, nil)
	testConfig.Lock()
	require.Equal(t, defaultValue, testConfig.v)
	testConfig.Unlock()

	// test nil default value
	vw.Update(newMockValue(t, &commonpb.Float64Proto{Value: 100}))
	<-w.C()
	UpdateStringArrayWithValue(w.Get(), "foo", &testConfig.v, &testConfig.RWMutex, nil, nil)
	testConfig.Lock()
	require.Nil(t, testConfig.v)
	testConfig.Unlock()
}

func TestUpdateTimeWithValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testConfig := struct {
		sync.RWMutex
		v time.Time
	}{}
	vw := kv.NewValueWatchable()
	_, w, _ := vw.Watch()

	defaultTime := time.Now().Add(time.Hour)
	t1 := time.Now()

	vw.Update(newMockValue(t, &commonpb.Int64Proto{Value: t1.Unix()}))
	<-w.C()
	UpdateTimeWithValue(w.Get(), "foo", &testConfig.v, &testConfig.RWMutex, defaultTime, nil)
	testConfig.Lock()
	require.Equal(t, t1.Unix(), testConfig.v.Unix())
	testConfig.Unlock()

	// mimic a bad value update, should fall back to default
	vw.Update(newMockValue(t, &commonpb.Float64Proto{Value: 20}))
	<-w.C()
	UpdateTimeWithValue(w.Get(), "foo", &testConfig.v, &testConfig.RWMutex, defaultTime, nil)
	testConfig.Lock()
	require.Equal(t, defaultTime, testConfig.v)
	testConfig.Unlock()

	// mimic a delete on kv store, should fall back to default
	vw.Update(nil)
	<-w.C()
	UpdateTimeWithValue(w.Get(), "foo", &testConfig.v, &testConfig.RWMutex, defaultTime, nil)
	testConfig.Lock()
	require.Equal(t, defaultTime, testConfig.v)
	testConfig.Unlock()
}

type mockValue struct {
	data []byte
}

func newMockValue(t *testing.T, msg proto.Message) *mockValue {
	b, err := proto.Marshal(msg)
	require.NoError(t, err)

	return &mockValue{data: b}
}

func (mv *mockValue) Unmarshal(msg proto.Message) error {
	return proto.Unmarshal(mv.data, msg)
}

func (mv *mockValue) Version() int { return 0 }

func (mv *mockValue) IsNewer(other kv.Value) bool {
	return mv.Version() > other.Version()
}
