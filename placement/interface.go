package placement

// Algorithm places shards on hosts
type Algorithm interface {
	// InitPlacement initialize a sharding placement with RF = 1
	InitPlacement(hosts []Host, ids []int) (Snapshot, error)

	// AddReplica up the RF by 1 in the cluster
	AddReplica(p Snapshot) (Snapshot, error)

	// AddHost adds a host to the cluster
	AddHost(p Snapshot, h Host) (Snapshot, error)

	// RemoveHost removes a host from the cluster
	RemoveHost(p Snapshot, h Host) (Snapshot, error)

	// ReplaceHost replace a host with a new host
	ReplaceHost(p Snapshot, leavingHost, addingHost Host) (Snapshot, error)
}

// DeploymentPlanner generates deployment steps for a placement
type DeploymentPlanner interface {
	// DeploymentSteps returns the deployment steps
	DeploymentSteps(p Snapshot) [][]HostShards
}

// Snapshot describes how shards are placed on hosts
type Snapshot interface {
	// HostShards returns all HostShards in the placement
	HostShards() []HostShards

	// HostsLen returns the length of all hosts in the placement
	HostsLen() int

	// Replicas returns the replication factor in the placement
	Replicas() int

	// ShardsLen returns the number of shards in a replica
	ShardsLen() int

	// ShardIDs returns all the unique shard ids for a replica
	Shards() []int
}

// HostShards represents a host and its assigned shards
type HostShards interface {
	Host() Host
	Shards() []int
}

// Host is where the shards are being placed
type Host interface {
	// Address returns the address of the host
	Address() string

	// Rack returns the rack of the host
	Rack() string
}
