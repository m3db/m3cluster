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

package storage

import (
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3x/log"
)

type w struct {
	w      kv.ValueWatch
	logger log.Logger
}

func newPlacementWatch(vw kv.ValueWatch, opts placement.Options) placement.Watch {
	return &w{
		w:      vw,
		logger: opts.InstrumentOptions().Logger(),
	}
}

func (w *w) C() <-chan struct{} {
	return w.w.C()
}

func (w *w) Get() placement.Placement {
	v := w.w.Get()
	if v == nil {
		w.logger.Errorf("received nil placement from kv")
		return nil
	}
	p, err := placementFromValue(v)
	if err != nil {
		w.logger.Errorf("could not process placement from kv with version %d, %v", v.Version(), err)
		return nil
	}
	w.logger.Infof("successfully processed placement from kv with version %d", v.Version())
	return p
}

func (w *w) Close() {
	w.w.Close()
}
