package services

import (
	"errors"

	"github.com/m3db/m3cluster/shard"
)

var errInstanceNotFound = errors.New("instance not found")

// NewService creates a new Service
func NewService() Service { return new(service) }

type service struct {
	instances   []ServiceInstance
	replication ServiceReplication
	sharding    ServiceSharding
}

func (s *service) Instance(instanceID string) (ServiceInstance, error) {
	for _, instance := range s.instances {
		if instance.ID() == instanceID {
			return instance, nil
		}
	}
	return nil, errInstanceNotFound
}
func (s *service) Instances() []ServiceInstance                 { return s.instances }
func (s *service) Replication() ServiceReplication              { return s.replication }
func (s *service) Sharding() ServiceSharding                    { return s.sharding }
func (s *service) SetInstances(insts []ServiceInstance) Service { s.instances = insts; return s }
func (s *service) SetReplication(r ServiceReplication) Service  { s.replication = r; return s }
func (s *service) SetSharding(ss ServiceSharding) Service       { s.sharding = ss; return s }

// NewServiceInstance creates a new ServiceInstance
func NewServiceInstance() ServiceInstance { return new(serviceInstance) }

type serviceInstance struct {
	id        string
	service   string
	zone      string
	endpoints []string
	shards    shard.Shards
}

func (i *serviceInstance) Service() string                          { return i.service }
func (i *serviceInstance) ID() string                               { return i.id }
func (i *serviceInstance) Zone() string                             { return i.zone }
func (i *serviceInstance) Endpoints() []string                      { return i.endpoints }
func (i *serviceInstance) Shards() shard.Shards                     { return i.shards }
func (i *serviceInstance) SetService(s string) ServiceInstance      { i.service = s; return i }
func (i *serviceInstance) SetID(id string) ServiceInstance          { i.id = id; return i }
func (i *serviceInstance) SetZone(z string) ServiceInstance         { i.zone = z; return i }
func (i *serviceInstance) SetEndpoints(e []string) ServiceInstance  { i.endpoints = e; return i }
func (i *serviceInstance) SetShards(s shard.Shards) ServiceInstance { i.shards = s; return i }

// NewServiceReplication creates a new ServiceReplication
func NewServiceReplication() ServiceReplication { return new(serviceReplication) }

type serviceReplication struct {
	replicas int
}

func (r *serviceReplication) Replicas() int                          { return r.replicas }
func (r *serviceReplication) SetReplicas(rep int) ServiceReplication { r.replicas = rep; return r }

// NewServiceSharding creates a new ServiceSharding
func NewServiceSharding() ServiceSharding { return new(serviceSharding) }

type serviceSharding struct {
	numShards int
}

func (s *serviceSharding) NumShards() int                     { return s.numShards }
func (s *serviceSharding) SetNumShards(n int) ServiceSharding { s.numShards = n; return s }

// NewAdvertisement creates a new Advertisement
func NewAdvertisement() Advertisement { return new(advertisement) }

type advertisement struct {
	id       string
	service  string
	endpoint string
	health   func() error
}

func (a *advertisement) ID() string                             { return a.id }
func (a *advertisement) Service() string                        { return a.service }
func (a *advertisement) Endpoint() string                       { return a.endpoint }
func (a *advertisement) Health() func() error                   { return a.health }
func (a *advertisement) SetID(id string) Advertisement          { a.id = id; return a }
func (a *advertisement) SetService(s string) Advertisement      { a.service = s; return a }
func (a *advertisement) SetEndpoint(e string) Advertisement     { a.endpoint = e; return a }
func (a *advertisement) SetHealth(h func() error) Advertisement { a.health = h; return a }

// NewServiceQuery creates new ServiceQuery
func NewServiceQuery() ServiceQuery { return new(serviceQuery) }

type serviceQuery struct {
	service string
	env     string
	zone    string
}

func (sq *serviceQuery) Service() string                      { return sq.service }
func (sq *serviceQuery) Environment() string                  { return sq.env }
func (sq *serviceQuery) Zone() string                         { return sq.zone }
func (sq *serviceQuery) SetService(s string) ServiceQuery     { sq.service = s; return sq }
func (sq *serviceQuery) SetEnvironment(e string) ServiceQuery { sq.env = e; return sq }
func (sq *serviceQuery) SetZone(z string) ServiceQuery        { sq.zone = z; return sq }

// NewQueryOptions creates new QueryOptions
func NewQueryOptions() QueryOptions { return new(queryOptions) }

type queryOptions struct {
	includeUnhealthy bool
}

func (qo *queryOptions) IncludeUnhealthy() bool                  { return qo.includeUnhealthy }
func (qo *queryOptions) SetIncludeUnhealthy(h bool) QueryOptions { qo.includeUnhealthy = h; return qo }
