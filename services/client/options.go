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

package client

import (
	"errors"
	"time"

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/services/heartbeat"
	"github.com/m3db/m3x/instrument"
)

const (
	defaultHBCheckInterval = 10 * time.Second
	defaultInitTimeout     = 5 * time.Second
)

var (
	errNoKVGen                  = errors.New("no KVGen function set")
	errNoHBGen                  = errors.New("no HBGen function set")
	errInvalidHeartbeatInterval = errors.New("non-positive heartbeat interval for heartbeat check")
	errInvalidHInitTimeout      = errors.New("non-positive init timeout for service watch")
)

// KVGen generates a kv store for a given zone
type KVGen func(zone string) (kv.Store, error)

// HBGen generates a heartbeat store for a given zone
type HBGen func(zone string) (heartbeat.Store, error)

// Options are options for the client of Services
type Options interface {
	// InitTimeout is the max time to wait on a new service watch for a valid initial value
	// If the value is set to 0, then no wait will be done and the watch could return empty value
	InitTimeout() time.Duration

	// SetInitTimeout sets the InitTimeout
	SetInitTimeout(t time.Duration) Options

	// HeartbeatCheckInterval is the interval for heartbeat check
	// for watches on healthy instances only
	HeartbeatCheckInterval() time.Duration

	// SetHeartbeatCheckInterval sets the HeartbeatCheckInterval
	SetHeartbeatCheckInterval(t time.Duration) Options

	// KVGen is the function to generate a kv store for a given zone
	KVGen() KVGen

	// SetKVGen sets the KVGen
	SetKVGen(gen KVGen) Options

	// HBGen is the function to generate a heartbeat store for a given zone
	HBGen() HBGen

	// SetHBGen sets the HBGen
	SetHBGen(gen HBGen) Options

	// InstrumentsOptions is the instrument options
	InstrumentsOptions() instrument.Options

	// SetInstrumentsOptions sets the InstrumentsOptions
	SetInstrumentsOptions(iopts instrument.Options) Options

	// Validate validates the Options
	Validate() error
}

type options struct {
	initTimeout time.Duration
	hbInterval  time.Duration
	kvGen       KVGen
	hbGen       HBGen
	iopts       instrument.Options
}

// NewOptions creates an Option
func NewOptions() Options {
	return options{
		iopts:       instrument.NewOptions(),
		hbInterval:  defaultHBCheckInterval,
		initTimeout: defaultInitTimeout,
	}
}

func (o options) Validate() error {
	if o.kvGen == nil {
		return errNoKVGen
	}

	if o.hbGen == nil {
		return errNoKVGen
	}

	if o.hbInterval == 0 {
		return errInvalidHeartbeatInterval
	}

	if o.initTimeout == 0 {
		return errInvalidHInitTimeout
	}

	return nil
}

func (o options) InitTimeout() time.Duration {
	return o.initTimeout
}

func (o options) SetInitTimeout(t time.Duration) Options {
	o.initTimeout = t
	return o
}

func (o options) HeartbeatCheckInterval() time.Duration {
	return o.hbInterval
}

func (o options) SetHeartbeatCheckInterval(t time.Duration) Options {
	o.hbInterval = t
	return o
}

func (o options) KVGen() KVGen {
	return o.kvGen
}

func (o options) SetKVGen(gen KVGen) Options {
	o.kvGen = gen
	return o
}

func (o options) HBGen() HBGen {
	return o.hbGen
}

func (o options) SetHBGen(gen HBGen) Options {
	o.hbGen = gen
	return o
}

func (o options) InstrumentsOptions() instrument.Options {
	return o.iopts
}

func (o options) SetInstrumentsOptions(iopts instrument.Options) Options {
	o.iopts = iopts
	return o
}
