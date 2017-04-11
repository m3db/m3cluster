package util

import (
	"errors"
	"sync"
	"time"

	"github.com/m3db/m3cluster/generated/proto/commonpb"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3x/log"
)

var errNilStore = errors.New("kv store is nil")

type getValueFn func(kv.Value) (interface{}, error)
type updateFn func(interface{})

// WatchAndUpdateBool sets up a watch for a bool property.
func WatchAndUpdateBool(store kv.Store, name string, property *bool, lock sync.Locker,
	defaultValue bool, logger xlog.Logger) error {
	if store == nil {
		return errNilStore
	}

	watch, err := store.Watch(name)
	if err != nil {
		return err
	}

	go func() {
		for range watch.C() {
			UpdateBoolWithValue(watch.Get(), name, property, lock, defaultValue, logger)
		}
	}()

	return nil
}

// UpdateBoolWithValue updates a bool with kv.Value
func UpdateBoolWithValue(
	v kv.Value,
	key string,
	prop *bool,
	lock sync.Locker,
	defaultValue bool,
	logger xlog.Logger,
) error {
	update := func(i interface{}) {
		lock.Lock()
		*prop = i.(bool)
		lock.Unlock()
	}

	return updateWithKV(getBool, update, key, v, defaultValue, logger)
}

// BoolFromValue get a bool from kv.Value
func BoolFromValue(v kv.Value, key string, defaultValue bool) bool {
	var res bool
	update := func(i interface{}) {
		res = i.(bool)
	}

	updateWithKV(getBool, update, key, v, defaultValue, nil)

	return res
}

func getBool(v kv.Value) (interface{}, error) {
	var boolProto commonpb.BoolProto
	err := v.Unmarshal(&boolProto)
	return boolProto.Value, err
}

// WatchAndUpdateFloat64 sets up a watch for an float64 property.
func WatchAndUpdateFloat64(store kv.Store, name string, property *float64, lock sync.Locker,
	defaultValue float64, logger xlog.Logger) error {
	if store == nil {
		return errNilStore
	}

	watch, err := store.Watch(name)
	if err != nil {
		return err
	}

	go func() {
		for range watch.C() {
			UpdateFloat64WithValue(watch.Get(), name, property, lock, defaultValue, logger)
		}
	}()

	return nil
}

// UpdateFloat64WithValue updates a float64 with kv.Value
func UpdateFloat64WithValue(
	v kv.Value,
	key string,
	prop *float64,
	lock sync.Locker,
	defaultValue float64,
	logger xlog.Logger,
) error {
	update := func(i interface{}) {
		lock.Lock()
		*prop = i.(float64)
		lock.Unlock()
	}

	return updateWithKV(getFloat64, update, key, v, defaultValue, logger)
}

// Float64FromValue gets an float64 from kv.Value
func Float64FromValue(v kv.Value, key string, defaultValue float64) float64 {
	var res float64
	update := func(i interface{}) {
		res = i.(float64)
	}

	updateWithKV(getFloat64, update, key, v, defaultValue, nil)

	return res
}

func getFloat64(v kv.Value) (interface{}, error) {
	var float64proto commonpb.Float64Proto
	err := v.Unmarshal(&float64proto)
	return float64proto.Value, err
}

// WatchAndUpdateInt64 sets up a watch for an int64 property.
func WatchAndUpdateInt64(store kv.Store, name string, property *int64, lock sync.Locker,
	defaultValue int64, logger xlog.Logger) error {
	if store == nil {
		return errNilStore
	}

	watch, err := store.Watch(name)
	if err != nil {
		return err
	}

	go func() {
		for range watch.C() {
			UpdateInt64WithValue(watch.Get(), name, property, lock, defaultValue, logger)
		}
	}()

	return nil
}

// UpdateInt64WithValue updates an int64 with kv.Value
func UpdateInt64WithValue(
	v kv.Value,
	key string,
	prop *int64,
	lock sync.Locker,
	defaultValue int64,
	logger xlog.Logger,
) error {
	update := func(i interface{}) {
		lock.Lock()
		*prop = i.(int64)
		lock.Unlock()
	}

	return updateWithKV(getInt64, update, key, v, defaultValue, logger)
}

// Int64FromValue gets an int64 from kv.Value
func Int64FromValue(v kv.Value, key string, defaultValue int64) int64 {
	var res int64
	update := func(i interface{}) {
		res = i.(int64)
	}

	updateWithKV(getInt64, update, key, v, defaultValue, nil)

	return res
}

func getInt64(v kv.Value) (interface{}, error) {
	var int64Proto commonpb.Int64Proto
	if err := v.Unmarshal(&int64Proto); err != nil {
		return 0, err
	}

	return int64Proto.Value, nil
}

// UpdateStringArrayWithValue updates a string array with kv.Value
func UpdateStringArrayWithValue(
	v kv.Value,
	key string,
	prop *[]string,
	lock sync.Locker,
	defaultValue []string,
	logger xlog.Logger,
) error {
	update := func(i interface{}) {
		lock.Lock()
		*prop = i.([]string)
		lock.Unlock()
	}

	return updateWithKV(getStringArray, update, key, v, defaultValue, logger)
}

// StringArrayFromValue gets a string array from kv.Value
func StringArrayFromValue(v kv.Value, key string, defaultValue []string) []string {
	var res []string
	update := func(i interface{}) {
		res = i.([]string)
	}

	updateWithKV(getStringArray, update, key, v, defaultValue, nil)

	return res
}

func getStringArray(v kv.Value) (interface{}, error) {
	var stringArrProto commonpb.StringArrayProto
	if err := v.Unmarshal(&stringArrProto); err != nil {
		return nil, err
	}

	return stringArrProto.Values, nil
}

// WatchAndUpdateTime sets up a watch for a time property.
func WatchAndUpdateTime(store kv.Store, name string, property *time.Time, lock sync.Locker,
	defaultValue time.Time, logger xlog.Logger) error {
	if store == nil {
		return errNilStore
	}

	watch, err := store.Watch(name)
	if err != nil {
		return err
	}

	go func() {
		for range watch.C() {
			UpdateTimeWithValue(watch.Get(), name, property, lock, defaultValue, logger)
		}
	}()

	return nil
}

// UpdateTimeWithValue updates a time with kv.Value
func UpdateTimeWithValue(
	v kv.Value,
	key string,
	prop *time.Time,
	lock sync.Locker,
	defaultValue time.Time,
	logger xlog.Logger,
) error {
	update := func(i interface{}) {
		lock.Lock()
		*prop = i.(time.Time)
		lock.Unlock()
	}
	return updateWithKV(getTime, update, key, v, defaultValue, logger)
}

func getTime(v kv.Value) (interface{}, error) {
	var int64Proto commonpb.Int64Proto
	if err := v.Unmarshal(&int64Proto); err != nil {
		return nil, err
	}

	return time.Unix(int64Proto.Value, 0), nil
}

func updateWithKV(
	getValue getValueFn,
	update updateFn,
	key string,
	v kv.Value,
	defaultValue interface{},
	logger xlog.Logger,
) error {
	if v == nil {
		// the key is deleted from kv, use the default value
		update(defaultValue)
		logSetDefault(logger, key, defaultValue)
		return nil
	}

	newValue, err := getValue(v)
	if err != nil {
		update(defaultValue)
		logInvalidUpdate(logger, key, v.Version(), defaultValue, err)
		return err
	}

	update(newValue)
	logUpdateSuccess(logger, key, v.Version(), newValue)
	return nil
}

func logSetDefault(logger xlog.Logger, k string, v interface{}) {
	getLogger(logger).WithFields(
		xlog.NewLogField("key", k),
		xlog.NewLogField("value", v),
	).Infof("nil value from kv store, use default value")
}

func logUpdateSuccess(logger xlog.Logger, k string, ver int, v interface{}) {
	getLogger(logger).WithFields(
		xlog.NewLogField("key", k),
		xlog.NewLogField("value", v),
		xlog.NewLogField("version", ver),
	).Infof("value update success")
}

func logInvalidUpdate(logger xlog.Logger, k string, ver int, v interface{}, err error) {
	getLogger(logger).WithFields(
		xlog.NewLogField("key", k),
		xlog.NewLogField("value", v),
		xlog.NewLogField("version", ver),
		xlog.NewLogField("error", err),
	).Infof("invalid value from kv store, use default value")
}

func getLogger(logger xlog.Logger) xlog.Logger {
	if logger == nil {
		return xlog.NullLogger
	}
	return logger
}
