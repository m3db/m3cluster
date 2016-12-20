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
	"sort"

	placementproto "github.com/m3db/m3cluster/generated/proto/placement"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/placement"
	"github.com/m3db/m3cluster/shard"
)

func serviceFromProto(p placementproto.Placement, sid services.ServiceID) services.Service {
	r := make([]services.ServiceInstance, 0, len(p.Instances))
	for _, instance := range p.Instances {
		r = append(r, serviceInstanceFromProto(instance, sid))
	}

	return services.NewService().
		SetReplication(services.NewServiceReplication().SetReplicas(int(p.ReplicaFactor))).
		SetSharding(services.NewServiceSharding().SetNumShards(int(p.NumShards))).
		SetInstances(r)
}

func serviceInstanceFromProto(instance *placementproto.Instance, sid services.ServiceID) services.ServiceInstance {
	return services.NewServiceInstance().
		SetServiceID(sid).
		SetInstanceID(instance.Id).
		SetEndpoint(instance.Endpoint).
		SetShards(shardsFromProto(instance.Shards))
}

// PlacementFromProto converts a placement proto to a ServicePlacement
func PlacementFromProto(p placementproto.Placement) (services.ServicePlacement, error) {
	shards := make([]uint32, p.NumShards)
	for i := uint32(0); i < p.NumShards; i++ {
		shards[i] = i
	}

	instances := make([]services.PlacementInstance, 0, len(p.Instances))
	for _, instance := range p.Instances {
		instances = append(
			instances,
			placement.NewInstance().
				SetID(instance.Id).
				SetRack(instance.Rack).
				SetWeight(instance.Weight).
				SetZone(instance.Zone).
				SetEndpoint(instance.Endpoint).
				SetShards(shardsFromProto(instance.Shards)),
		)
	}

	s := placement.NewPlacement(instances, shards, int(p.ReplicaFactor))
	if err := placement.Validate(s); err != nil {
		return nil, err
	}

	return s, nil
}

// PlacementToProto converts a ServicePlacement to a placement proto
func PlacementToProto(p services.ServicePlacement) placementproto.Placement {
	instances := make(map[string]*placementproto.Instance, p.NumInstances())
	for _, instance := range p.Instances() {
		shards := sortableProtoShards(shardsToProto(instance.Shards()))
		sort.Sort(shards)
		instances[instance.ID()] = &placementproto.Instance{
			Id:       instance.ID(),
			Rack:     instance.Rack(),
			Zone:     instance.Zone(),
			Weight:   instance.Weight(),
			Endpoint: instance.Endpoint(),
			Shards:   shards,
		}
	}
	return placementproto.Placement{
		Instances:     instances,
		ReplicaFactor: uint32(p.ReplicaFactor()),
		NumShards:     uint32(p.NumShards()),
	}
}

func shardsToProto(shards shard.Shards) []*placementproto.Shard {
	r := make([]*placementproto.Shard, shards.NumShards())
	for i, s := range shards.All() {
		r[i] = &placementproto.Shard{
			Id:       s.ID(),
			State:    shardStateToProto(s.State()),
			SourceId: s.SourceID(),
		}
	}
	return r
}

func shardsFromProto(protoshards []*placementproto.Shard) shard.Shards {
	shards := make([]shard.Shard, len(protoshards))
	for i, s := range protoshards {
		shards[i] = shardFromProto(s)
	}
	return shard.NewShards(shards)
}

func shardFromProto(s *placementproto.Shard) shard.Shard {
	return shard.NewShard(s.Id).SetState(shardStateFromProto(s.State)).SetSourceID(s.SourceId)
}

func shardStateFromProto(s placementproto.ShardState) shard.State {
	if s == placementproto.ShardState_Initializing {
		return shard.Initializing
	} else if s == placementproto.ShardState_Available {
		return shard.Available
	} else if s == placementproto.ShardState_Leaving {
		return shard.Leaving
	}

	panic("unknown shard state from proto")
}

func shardStateToProto(s shard.State) placementproto.ShardState {
	if s == shard.Initializing {
		return placementproto.ShardState_Initializing
	} else if s == shard.Available {
		return placementproto.ShardState_Available
	} else if s == shard.Leaving {
		return placementproto.ShardState_Leaving
	}

	panic("unknown shard state to proto")
}

type sortableProtoShards []*placementproto.Shard

func (su sortableProtoShards) Len() int {
	return len(su)
}

func (su sortableProtoShards) Less(i, j int) bool {
	return su[i].Id < su[j].Id
}

func (su sortableProtoShards) Swap(i, j int) {
	su[i], su[j] = su[j], su[i]
}
