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

package services

import (
	"github.com/m3db/m3cluster/shard"
	xwatch "github.com/m3db/m3x/watch"
)

// Service describes the metadata and instances of a service
type Service interface {
	// Instance returns the service instance with the instance id
	Instance(instanceID string) (ServiceInstance, error)

	// Instances returns the service instances
	Instances() []ServiceInstance

	// SetInstances sets the service instances
	SetInstances(insts []ServiceInstance) Service

	// Replication returns the service replication description or nil if none
	Replication() ServiceReplication

	// SetReplication sets the service replication description or nil if none
	SetReplication(r ServiceReplication) Service

	// Sharding returns the service sharding description or nil if none
	Sharding() ServiceSharding

	// SetSharding sets the service sharding description or nil if none
	SetSharding(s ServiceSharding) Service
}

// PlacementSnapshot describes how shards are placed on instances
type PlacementSnapshot interface {
	// Instances returns the placement instances
	Instances() []PlacementInstance

	// SetInstances sets the placement instances
	SetInstances(insts []PlacementInstance) PlacementSnapshot

	// Replication returns the placement replication description or nil if none
	Replication() ServiceReplication

	// SetReplication sets the placement replication description or nil if none
	SetReplication(r ServiceReplication) PlacementSnapshot

	// Sharding returns the placement sharding description or nil if none
	Sharding() ServiceSharding

	// SetSharding sets the placement sharding description or nil if none
	SetSharding(s ServiceSharding) PlacementSnapshot

	// Validate checks if the placement is valid
	Validate() error
}

// ServiceReplication describes the replication of a service
type ServiceReplication interface {
	// Replicas is the count of replicas
	Replicas() int

	// SetReplicas sets the count of replicas
	SetReplicas(r int) ServiceReplication
}

// ServiceSharding describes the sharding of a service
type ServiceSharding interface {
	// NumShards is the number of shards to use for sharding
	NumShards() int

	// SetNumShards sets the number of shards to use for sharding
	SetNumShards(n int) ServiceSharding
}

// ServiceInstance is a single instance of a service
type ServiceInstance interface {
	PlacementInstance() string
	SetPlacementInstance(i PlacementInstance) ServiceInstance
	Service() string                         // the service implemented by the instance
	SetService(s string) ServiceInstance     // sets the service implemented by the instance
	Endpoints() []string                     // Endpoint addresses for contacting the instance
	SetEndpoints(e []string) ServiceInstance // sets the endpoint addresses for the instance
}

// PlacementInstance is a single instance in a placement
type PlacementInstance interface {
	ID() string                                 // ID of the instance
	SetID(id string) PlacementInstance          // sets the ID of the instance
	Zone() string                               // Zone in which the instance resides
	SetZone(z string) PlacementInstance         // sets the zone in which the instance resides
	Shards() shard.Shards                       // Shards owned by the instance
	SetShards(s shard.Shards) PlacementInstance // sets the Shards assigned to the instance
	// weight
	// rack
}

// Advertisement advertises the availability of a given instance of a service
type Advertisement interface {
	ID() string                                  // the ID of the instance being advertised
	SetID(id string) Advertisement               // sets the ID being advertised
	Service() string                             // the service being advertised
	SetService(service string) Advertisement     // sets the service being advertised
	Health() func() error                        // optional health function.  return an error to indicate unhealthy
	SetHealth(health func() error) Advertisement // sets the health function for the advertised instance
	Endpoint() string                            // endpoint exposed by the service
	SetEndpoint(e string) Advertisement          // sets the endpoint exposed by the service
}

// ServiceQuery contains the fields required for service discovery queries
type ServiceQuery interface {
	Service() string                        // the service name of the query
	SetService(s string) ServiceQuery       // set the service name of the query
	Environment() string                    // the environemnt of the query
	SetEnvironment(env string) ServiceQuery // sets the environemnt of the query
	Zone() string                           // the zone of the query
	SetZone(zone string) ServiceQuery       // sets the zone of the query
}

// QueryOptions are options to service discovery queries
type QueryOptions interface {
	IncludeUnhealthy() bool                  // if true, will return unhealthy instances
	SetIncludeUnhealthy(h bool) QueryOptions // sets whether to include unhealthy instances
}

// Services provides access to the service topology
type Services interface {
	// Advertise advertises the availability of an instance of a service
	Advertise(ad Advertisement) error

	// Unadvertise indicates a given instance is no longer available
	Unadvertise(service ServiceQuery, id string) error

	// Query returns metadata and a list of available instances for a given service
	Query(service ServiceQuery, opts QueryOptions) (Service, error)

	// Watch returns a watch on metadata and a list of available instances for a given service
	Watch(service ServiceQuery, opts QueryOptions) (xwatch.Watch, error)
}

// PlacementService handles the placement related operations for registered services
// all write or update operations will persist the generated snapshot before returning success
type PlacementService interface {
	BuildInitialPlacement(service ServiceQuery, hosts []PlacementInstance, shardLen int, rf int) (PlacementSnapshot, error)
	AddReplica(service ServiceQuery) (PlacementSnapshot, error)
	AddHost(service ServiceQuery, candidateHosts []PlacementInstance) (PlacementSnapshot, error)
	RemoveHost(service ServiceQuery, host PlacementInstance) (PlacementSnapshot, error)
	ReplaceHost(service ServiceQuery, leavingHost PlacementInstance, candidateHosts []PlacementInstance) (PlacementSnapshot, error)

	// Snapshot gets the persisted snapshot for service
	Snapshot(service ServiceQuery) (PlacementSnapshot, error)
}

// PlacementStorage provides read and write access to placement snapshots
type PlacementStorage interface {
	SaveSnapshotForService(service ServiceQuery, p PlacementSnapshot) error
	ReadSnapshotForService(service ServiceQuery) (PlacementSnapshot, error)
}
