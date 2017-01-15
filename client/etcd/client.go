package etcd

import (
	"fmt"
	"sync"

	"github.com/coreos/etcd/clientv3"
	"github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/kv"
	etcdKV "github.com/m3db/m3cluster/kv/etcd"
	"github.com/m3db/m3cluster/services"
	sdClient "github.com/m3db/m3cluster/services/client"
	"github.com/m3db/m3cluster/services/heartbeat"
	etcdHeartbeat "github.com/m3db/m3cluster/services/heartbeat/etcd"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/log"
	"github.com/uber-go/tally"
)

const (
	keyFormat       = "%s/%s"
	cacheFileFormat = "%s-%s.json"
	kvPrefix        = "_kv"
)

// NewConfigServiceClient returns a ConfigServiceClient
func NewConfigServiceClient(opts Options) client.Client {
	return &csclient{
		opts:    opts,
		kvScope: opts.InstrumentOptions().MetricsScope().Tagged(map[string]string{"config_service": "kv"}),
		sdScope: opts.InstrumentOptions().MetricsScope().Tagged(map[string]string{"config_service": "sd"}),
		clis:    make(map[string]*clientv3.Client),
		logger:  opts.InstrumentOptions().Logger(),
	}

}

type csclient struct {
	sync.RWMutex
	clis map[string]*clientv3.Client

	opts    Options
	kvScope tally.Scope
	sdScope tally.Scope
	logger  xlog.Logger

	sdOnce sync.Once
	sd     services.Services
	sdErr  error

	kvOnce sync.Once
	kv     kv.Store
	kvErr  error
}

func (c *csclient) Services() (services.Services, error) {
	c.sdOnce.Do(func() {
		c.sd, c.sdErr = c.newServices()
	})

	return c.sd, c.sdErr
}

func (c *csclient) KV() (kv.Store, error) {
	c.kvOnce.Do(func() {
		c.kv, c.kvErr = c.newKVStore()
	})

	return c.kv, c.kvErr
}

func (c *csclient) newServices() (services.Services, error) {
	return sdClient.NewServices(sdClient.NewOptions().
		SetInitTimeout(c.opts.ServiceInitTimeout()).
		SetHeartbeatGen(c.heartbeatGen()).
		SetKVGen(c.kvGen(etcdKV.NewOptions().
			SetInstrumentsOptions(instrument.NewOptions().
				SetLogger(c.logger).
				SetMetricsScope(c.kvScope),
			)),
		).
		SetInstrumentsOptions(instrument.NewOptions().
			SetLogger(c.logger).
			SetMetricsScope(c.sdScope),
		),
	)
}

func (c *csclient) newKVStore() (kv.Store, error) {
	env := c.opts.Env()
	kvGen := c.kvGen(etcdKV.NewOptions().
		SetInstrumentsOptions(instrument.NewOptions().
			SetLogger(c.logger).
			SetMetricsScope(c.kvScope),
		).
		SetKeyFn(func(key string) string {
			if env != "" {
				key = fmt.Sprintf(keyFormat, env, key)
			}
			return fmt.Sprintf(keyFormat, kvPrefix, key)
		}),
	)
	return kvGen(c.opts.Zone())
}

func (c *csclient) kvGen(kvOpts etcdKV.Options) sdClient.KVGen {
	return sdClient.KVGen(
		func(zone string) (kv.Store, error) {
			cli, err := c.etcdClientGen(zone)
			if err != nil {
				return nil, err
			}

			return etcdKV.NewStore(
				cli,
				kvOpts.SetCacheFilePath(cacheFileForZone(c.opts.CacheDir(), c.opts.AppID(), zone)),
			), nil
		},
	)
}

func (c *csclient) heartbeatGen() sdClient.HeartbeatGen {
	return sdClient.HeartbeatGen(
		func(zone string) (heartbeat.Store, error) {
			cli, err := c.etcdClientGen(zone)
			if err != nil {
				return nil, err
			}

			return etcdHeartbeat.NewStore(cli), nil
		},
	)
}

func (c *csclient) etcdClientGen(zone string) (*clientv3.Client, error) {
	c.Lock()
	defer c.Unlock()

	cli, ok := c.clis[zone]
	if ok {
		return cli, nil
	}

	cluster, ok := c.opts.ClusterForZone(zone)
	if !ok {
		return nil, fmt.Errorf("no etcd cluster found for zone %s", zone)
	}

	cli, err := clientv3.New(clientv3.Config{Endpoints: cluster.Endpoints()})
	if err != nil {
		return nil, err
	}

	c.clis[zone] = cli
	return cli, nil
}

func cacheFileForZone(cacheDir, appID, zone string) string {
	if cacheDir == "" {
		return ""
	}
	if appID == "" {
		return ""
	}
	if zone == "" {
		return ""
	}
	cacheFileName := fmt.Sprintf(cacheFileFormat, appID, zone)
	return fmt.Sprintf(keyFormat, cacheDir, cacheFileName)
}
