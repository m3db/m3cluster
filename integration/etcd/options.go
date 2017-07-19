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

package etcd

import (
	"time"

	"github.com/m3db/m3x/instrument"
)

var (
	defaulTimeout = 5 * time.Second
	defaultDir    = "etcd.dir"
)

type opts struct {
	iopts       instrument.Options
	workingDir  string
	initTimeout time.Duration
}

// NewOptions returns a new options
func NewOptions() Options {
	return &opts{
		iopts:       instrument.NewOptions(),
		workingDir:  defaultDir,
		initTimeout: defaulTimeout,
	}
}

func (o *opts) SetInstrumentOptions(value instrument.Options) Options {
	oo := *o
	oo.iopts = value
	return &oo
}

func (o *opts) InstrumentOptions() instrument.Options {
	return o.iopts
}

func (o *opts) SetDir(value string) Options {
	oo := *o
	oo.workingDir = value
	return &oo
}

func (o *opts) Dir() string {
	return o.workingDir
}

func (o *opts) SetInitTimeout(value time.Duration) Options {
	oo := *o
	oo.initTimeout = value
	return &oo
}

func (o *opts) InitTimeout() time.Duration {
	return o.initTimeout
}
