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
	"time"

	"github.com/m3db/m3x/instrument"
)

const (
	defaultInitWatchTimeout = 10 * time.Second
)

// Options provide a set of value options.
type Options interface {
	// SetInstrumentOptions sets the instrument options.
	SetInstrumentOptions(value instrument.Options) Options

	// InstrumentOptions returns the instrument options.
	InstrumentOptions() instrument.Options

	// SetInitWatchTimeout sets the initial watch timeout.
	SetInitWatchTimeout(value time.Duration) Options

	// InitWatchTimeout returns the initial watch timeout.
	InitWatchTimeout() time.Duration

	// SetUpdatableFn sets the updatable function.
	SetUpdatableFn(value NewUpdatableFn) Options

	// UpdatableFn returns the updatable function.
	UpdatableFn() NewUpdatableFn

	// SetGetFn sets the get function.
	SetGetFn(value GetFn) Options

	// GetFn returns the get function.
	GetFn() GetFn

	// SetProcessFn sets the process function.
	SetProcessFn(value ProcessFn) Options

	// ProcessFn returns the process function.
	ProcessFn() ProcessFn
}

type options struct {
	instrumentOpts   instrument.Options
	initWatchTimeout time.Duration
	updatableFn      NewUpdatableFn
	getFn            GetFn
	processFn        ProcessFn
}

// NewOptions creates a new set of options.
func NewOptions() Options {
	return &options{
		instrumentOpts:   instrument.NewOptions(),
		initWatchTimeout: defaultInitWatchTimeout,
	}
}

func (o *options) SetInstrumentOptions(value instrument.Options) Options {
	opts := *o
	opts.instrumentOpts = value
	return &opts
}

func (o *options) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

func (o *options) SetInitWatchTimeout(value time.Duration) Options {
	opts := *o
	opts.initWatchTimeout = value
	return &opts
}

func (o *options) InitWatchTimeout() time.Duration {
	return o.initWatchTimeout
}

func (o *options) SetUpdatableFn(value NewUpdatableFn) Options {
	opts := *o
	opts.updatableFn = value
	return &opts
}

func (o *options) UpdatableFn() NewUpdatableFn {
	return o.updatableFn
}

func (o *options) SetGetFn(value GetFn) Options {
	opts := *o
	opts.getFn = value
	return &opts
}

func (o *options) GetFn() GetFn {
	return o.getFn
}

func (o *options) SetProcessFn(value ProcessFn) Options {
	opts := *o
	opts.processFn = value
	return &opts
}

func (o *options) ProcessFn() ProcessFn {
	return o.processFn
}
