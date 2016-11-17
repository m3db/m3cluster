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

package placement

import "fmt"

// Algorithm places shards on hosts
type Algorithm interface {
	// InitPlacement initialize a sharding placement with RF = 1
	BuildInitialPlacement(hosts []Host, shards []uint32) (Snapshot, error)

	// AddReplica up the RF by 1 in the cluster
	AddReplica(p Snapshot) (Snapshot, error)

	// AddHost adds a host to the cluster
	AddHost(p Snapshot, h Host) (Snapshot, error)

	// RemoveHost removes a host from the cluster
	RemoveHost(p Snapshot, h Host) (Snapshot, error)

	// ReplaceHost replace a host with new hosts
	ReplaceHost(p Snapshot, leavingHost Host, addingHosts []Host) (Snapshot, error)
}

// HostShards represents a host and its assigned shards
type HostShards interface {
	Host() Host
	Shards() []uint32
	ShardsLen() int
	AddShard(shard uint32)
	RemoveShard(shard uint32)
	ContainsShard(shard uint32) bool
}

// Host represents a weighted host
type Host interface {
	fmt.Stringer
	ID() string
	Rack() string
	Zone() string
	Weight() uint32
}

// Options is the interface for placement options
type Options interface {
	// LooseRackCheck enables the placement to loose the rack check
	// during host replacement to achieve full ownership transfer
	LooseRackCheck() bool
	SetLooseRackCheck(looseRackCheck bool) Options

	// AllowPartialReplace allows shards from the leaving host to be
	// placed on hosts other than the new hosts in a replace operation
	AllowPartialReplace() bool
	SetAllowPartialReplace(allowPartialReplace bool) Options
}

// DeploymentOptions provides options for DeploymentPlanner
type DeploymentOptions interface {
	// MaxStepSize limits the number of hosts to be deployed in one step
	MaxStepSize() int
	SetMaxStepSize(stepSize int) DeploymentOptions
}

// DeploymentPlanner generates deployment steps for a placement
type DeploymentPlanner interface {
	// DeploymentSteps returns the deployment steps
	DeploymentSteps(p Snapshot) [][]HostShards
}
