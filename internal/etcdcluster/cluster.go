// Package etcdcluster provides functionality for creating an etcd cluster using
// docker-compose.
package etcdcluster

import (
	"context"
	"fmt"
	"os/exec"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
)

const (
	containerFmt = "etcd%d" // defined by docker-compose yaml
)

func containerName(id int) string {
	return fmt.Sprintf(containerFmt, id)
}

// Cluster defines a docker-compose based etcd cluster.
type Cluster struct {
	mu     sync.Mutex
	client *clientv3.Client
}

// Options configures the cluster.
type Options struct {
}

// New creates a new unstarted cluster based on opts.
func New(opts Options) (*Cluster, error) {
	return &Cluster{}, nil
}

func commandCtxTimeout(timeout time.Duration, command string, args ...string) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	cmd := exec.CommandContext(ctx, command, args...)
	_, err := cmd.Output() // using Output will populate err.Stderr
	cancel()

	return err
}

// Start spins up the underlying docker instances. It is not guaranteed that the
// etcd cluster is ready when Start returns.
func (c *Cluster) Start() error {
	return commandCtxTimeout(30*time.Second, "docker-compose", "up", "-d")
}

// Stop gracefully kills (SIGTERM) a specific etcd instance.
func (c *Cluster) Stop(id int) error {
	return commandCtxTimeout(30*time.Second, "docker-compose", "kill", "-s", "SIGTERM", containerName(id))
}

// HardStop kills a specific etcd instance without graceful shutdown.
func (c *Cluster) HardStop(id int) error {
	return commandCtxTimeout(30*time.Second, "docker-compose", "kill", containerName(id))
}

// KillLeader kills the current etcd / raft leader. If graceful is true the
// leader will be gracefully stopped, giving it a chance to transfer leadership
// to another node internally.
func (c *Cluster) KillLeader(graceful bool) error {
	ldID, err := c.leader()
	if err != nil {
		return err
	}

	if graceful {
		return c.Stop(ldID)
	}

	return c.HardStop(ldID)
}

// Shutdown stops the cluster and deletes all associated resources.
func (c *Cluster) Shutdown() error {
	c.mu.Lock()

	if c.client != nil {
		c.client.Close()
		c.client = nil
	}

	c.mu.Unlock()

	return commandCtxTimeout(30*time.Second, "docker-compose", "down", "-v")
}

// Client returns a client of the cluster.
func (c *Cluster) Client() (*clientv3.Client, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.client != nil {
		return c.client, nil
	}

	cl, err := clientv3.New(clientv3.Config{
		Endpoints:   c.endpoints(),
		DialTimeout: 5 * time.Second,
	})

	if err != nil {
		return nil, err
	}

	c.client = cl
	return cl, nil
}

func (c *Cluster) leader() (int, error) {
	cl, err := c.Client()
	if err != nil {
		return 0, err
	}

	leader := -1
	for i, ept := range c.endpoints() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		status, err := cl.Status(ctx, ept)
		cancel()

		if err != nil {
			return 0, err
		}

		if status.Header.MemberId == status.Leader {
			leader = i
			break
		}
	}

	return leader, nil
}

func (c *Cluster) endpoints() []string {
	return []string{"127.0.0.1:2370", "127.0.0.1:2371", "127.0.0.1:2372"}
}

// WaitReady blocks until all nodes are healthy. It returns an error if they are
// not ready within timeout.
func (c *Cluster) WaitReady(timeout time.Duration) error {
	var (
		wg    sync.WaitGroup
		errs  []error
		errMu sync.Mutex
	)

	for _, ept := range c.endpoints() {
		wg.Add(1)
		go func(ept string) {
			if err := waitEndpoint(ept, timeout); err != nil {
				errMu.Lock()
				errs = append(errs, err)
				errMu.Unlock()
			}
			wg.Done()
		}(ept)
	}

	wg.Wait()

	errMu.Lock()
	defer errMu.Unlock()

	if len(errs) > 0 {
		return fmt.Errorf("errors encountered waiting for endpoints: %v", errs)
	}
	return nil
}

func waitEndpoint(endpoint string, timeout time.Duration) error {
	cl, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 5 * time.Second,
	})

	if err != nil {
		return err
	}

	to := time.After(timeout)

	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		// getting a random key with quorum reads is enough to determine if node
		// is healthy
		_, err := cl.Get(ctx, "health")
		cancel()

		select {
		case <-to:
			return fmt.Errorf("endpoint %s not healthy within %s", endpoint, timeout.String())
		default:
			if err != nil {
				// retry until timeout
				continue
			} else {
				// got value successfully, return
				return nil
			}
		}
	}
}
