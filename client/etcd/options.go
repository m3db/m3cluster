package etcd

import "time"
import "github.com/m3db/m3x/instrument"

// Options is the Options to create a config service client
type Options interface {
	Env() string
	SetEnv(e string) Options

	Zone() string
	SetZone(z string) Options

	ServiceInitTimeout() time.Duration
	SetServiceInitTimeout(timeout time.Duration) Options

	CacheDir() string
	SetCacheDir(dir string) Options

	AppID() string
	SetAppID(id string) Options

	Clusters() []Cluster
	SetClusters(clusters []Cluster) Options
	ClusterForZone(z string) (Cluster, bool)

	InstrumentOptions() instrument.Options
	SetInstrumentOptions(iopts instrument.Options) Options
}

// Cluster defines the configuration for a etcd cluster
type Cluster interface {
	Zone() string
	SetZone(z string) Cluster

	Endpoints() []string
	SetEndpoints(endpoints []string) Cluster
}

// NewOptions creates a set of Options
func NewOptions() Options {
	return options{iopts: instrument.NewOptions()}
}

type options struct {
	env                string
	zone               string
	serviceInitTimeout time.Duration
	cacheDir           string
	appID              string
	clusters           map[string]Cluster
	iopts              instrument.Options
}

func (o options) Env() string {
	return o.env
}
func (o options) SetEnv(e string) Options {
	o.env = e
	return o
}

func (o options) Zone() string {
	return o.zone
}
func (o options) SetZone(z string) Options {
	o.zone = z
	return o
}

func (o options) ServiceInitTimeout() time.Duration {
	return o.serviceInitTimeout
}
func (o options) SetServiceInitTimeout(timeout time.Duration) Options {
	o.serviceInitTimeout = timeout
	return o
}

func (o options) CacheDir() string {
	return o.cacheDir
}

func (o options) SetCacheDir(dir string) Options {
	o.cacheDir = dir
	return o
}

func (o options) AppID() string {
	return o.appID
}
func (o options) SetAppID(id string) Options {
	o.appID = id
	return o
}

func (o options) Clusters() []Cluster {
	res := make([]Cluster, 0, len(o.clusters))
	for _, c := range o.clusters {
		res = append(res, c)
	}
	return res
}

func (o options) SetClusters(clusters []Cluster) Options {
	o.clusters = make(map[string]Cluster, len(clusters))
	for _, c := range clusters {
		o.clusters[c.Zone()] = c
	}
	return o
}

func (o options) ClusterForZone(z string) (Cluster, bool) {
	c, ok := o.clusters[z]
	return c, ok
}

func (o options) InstrumentOptions() instrument.Options {
	return o.iopts
}

func (o options) SetInstrumentOptions(iopts instrument.Options) Options {
	o.iopts = iopts
	return o
}

type cluster struct {
	zone      string
	endpoints []string
}

// NewCluster creates a Cluster
func NewCluster() Cluster {
	return cluster{}
}

func (c cluster) Zone() string {
	return c.zone
}

func (c cluster) SetZone(z string) Cluster {
	c.zone = z
	return c
}

func (c cluster) Endpoints() []string {
	return c.endpoints
}

func (c cluster) SetEndpoints(endpoints []string) Cluster {
	c.endpoints = endpoints
	return c
}
