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

package watchhelper

import (
	"errors"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/m3db/m3x/instrument"
)

const (
	defaultWatchChanCheckInterval = 10 * time.Second
	defaultWatchChanResetInterval = 10 * time.Second
	defaultWatchChanInitTimeout   = 10 * time.Second
)

var (
	errNilWatch                  = errors.New("invalid optiosn: nil watcher")
	errNilUpdateFn               = errors.New("invalid options: nil updateFn")
	errNilCheckAndStopFn         = errors.New("invalid options: nil checkAndStopFn")
	errNilInstrumentOptions      = errors.New("invalid optiosn: nil instrument options")
	errInvalidWatchCheckInterval = errors.New("invalid watch channel check interval")
)

// NewOptions creates sane options
func NewOptions() Options {
	return &options{
		watchChanCheckInterval: defaultWatchChanCheckInterval,
		watchChanResetInterval: defaultWatchChanResetInterval,
		watchChanInitTimeout:   defaultWatchChanInitTimeout,
		iopts:                  instrument.NewOptions(),
	}
}

type options struct {
	watcher        clientv3.Watcher
	updateFn       UpdateFn
	checkAndStopFn CheckAndStopFn

	wopts                  []clientv3.OpOption
	watchChanCheckInterval time.Duration
	watchChanResetInterval time.Duration
	watchChanInitTimeout   time.Duration
	iopts                  instrument.Options
}

func (o *options) Watcher() clientv3.Watcher {
	return o.watcher
}

func (o *options) SetWatcher(w clientv3.Watcher) Options {
	opts := *o
	opts.watcher = w
	return &opts
}

func (o *options) WatchChanCheckInterval() time.Duration {
	return o.watchChanCheckInterval
}

func (o *options) SetWatchChanCheckInterval(t time.Duration) Options {
	opts := *o
	opts.watchChanCheckInterval = t
	return &opts
}

func (o *options) WatchChanResetInterval() time.Duration {
	return o.watchChanResetInterval
}

func (o *options) SetWatchChanResetInterval(t time.Duration) Options {
	opts := *o
	opts.watchChanResetInterval = t
	return &opts

}

func (o *options) WatchChanInitTimeout() time.Duration {
	return o.watchChanInitTimeout
}

func (o *options) SetWatchChanInitTimeout(t time.Duration) Options {
	opts := *o
	opts.watchChanInitTimeout = t
	return &opts
}

func (o *options) UpdateFn() UpdateFn {
	return o.updateFn
}

func (o *options) SetUpdateFn(f UpdateFn) Options {
	opts := *o
	opts.updateFn = f
	return &opts

}

func (o *options) CheckAndStopFn() CheckAndStopFn {
	return o.checkAndStopFn
}

func (o *options) SetCheckAndStopFn(f CheckAndStopFn) Options {
	opts := *o
	opts.checkAndStopFn = f
	return &opts
}

func (o *options) WatchOptions() []clientv3.OpOption {
	return o.wopts
}

func (o *options) SetWatchOptions(options []clientv3.OpOption) Options {
	opts := *o
	opts.wopts = options
	return &opts
}

func (o *options) InstrumentsOptions() instrument.Options {
	return o.iopts
}

func (o *options) SetInstrumentsOptions(iopts instrument.Options) Options {
	opts := *o
	opts.iopts = iopts
	return &opts
}

func (o *options) Validate() error {
	if o.watcher == nil {
		return errNilWatch
	}

	if o.updateFn == nil {
		return errNilUpdateFn
	}

	if o.checkAndStopFn == nil {
		return errNilCheckAndStopFn
	}

	if o.iopts == nil {
		return errNilInstrumentOptions
	}

	if o.watchChanCheckInterval <= 0 {
		return errInvalidWatchCheckInterval
	}

	return nil
}
