// Copyright (c) 2018 Uber Technologies, Inc.
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

// Automatically generated by MockGen. DO NOT EDIT!
// Source: github.com/m3db/m3cluster/services/types.go

package services

import (
	"time"

	"github.com/m3db/m3cluster/generated/proto/metadatapb"
	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3cluster/services/leader/campaign"
	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3x/watch"

	"github.com/golang/mock/gomock"
)

// Mock of Options interface
type MockOptions struct {
	ctrl     *gomock.Controller
	recorder *_MockOptionsRecorder
}

// Recorder for MockOptions (not exported)
type _MockOptionsRecorder struct {
	mock *MockOptions
}

func NewMockOptions(ctrl *gomock.Controller) *MockOptions {
	mock := &MockOptions{ctrl: ctrl}
	mock.recorder = &_MockOptionsRecorder{mock}
	return mock
}

func (_m *MockOptions) EXPECT() *_MockOptionsRecorder {
	return _m.recorder
}

func (_m *MockOptions) NamespaceOptions() NamespaceOptions {
	ret := _m.ctrl.Call(_m, "NamespaceOptions")
	ret0, _ := ret[0].(NamespaceOptions)
	return ret0
}

func (_mr *_MockOptionsRecorder) NamespaceOptions() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "NamespaceOptions")
}

func (_m *MockOptions) SetNamespaceOptions(opts NamespaceOptions) Options {
	ret := _m.ctrl.Call(_m, "SetNamespaceOptions", opts)
	ret0, _ := ret[0].(Options)
	return ret0
}

func (_mr *_MockOptionsRecorder) SetNamespaceOptions(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "SetNamespaceOptions", arg0)
}

// Mock of NamespaceOptions interface
type MockNamespaceOptions struct {
	ctrl     *gomock.Controller
	recorder *_MockNamespaceOptionsRecorder
}

// Recorder for MockNamespaceOptions (not exported)
type _MockNamespaceOptionsRecorder struct {
	mock *MockNamespaceOptions
}

func NewMockNamespaceOptions(ctrl *gomock.Controller) *MockNamespaceOptions {
	mock := &MockNamespaceOptions{ctrl: ctrl}
	mock.recorder = &_MockNamespaceOptionsRecorder{mock}
	return mock
}

func (_m *MockNamespaceOptions) EXPECT() *_MockNamespaceOptionsRecorder {
	return _m.recorder
}

func (_m *MockNamespaceOptions) PlacementNamespace() string {
	ret := _m.ctrl.Call(_m, "PlacementNamespace")
	ret0, _ := ret[0].(string)
	return ret0
}

func (_mr *_MockNamespaceOptionsRecorder) PlacementNamespace() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "PlacementNamespace")
}

func (_m *MockNamespaceOptions) SetPlacementNamespace(v string) NamespaceOptions {
	ret := _m.ctrl.Call(_m, "SetPlacementNamespace", v)
	ret0, _ := ret[0].(NamespaceOptions)
	return ret0
}

func (_mr *_MockNamespaceOptionsRecorder) SetPlacementNamespace(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "SetPlacementNamespace", arg0)
}

func (_m *MockNamespaceOptions) MetadataNamespace() string {
	ret := _m.ctrl.Call(_m, "MetadataNamespace")
	ret0, _ := ret[0].(string)
	return ret0
}

func (_mr *_MockNamespaceOptionsRecorder) MetadataNamespace() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "MetadataNamespace")
}

func (_m *MockNamespaceOptions) SetMetadataNamespace(v string) NamespaceOptions {
	ret := _m.ctrl.Call(_m, "SetMetadataNamespace", v)
	ret0, _ := ret[0].(NamespaceOptions)
	return ret0
}

func (_mr *_MockNamespaceOptionsRecorder) SetMetadataNamespace(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "SetMetadataNamespace", arg0)
}

// Mock of Services interface
type MockServices struct {
	ctrl     *gomock.Controller
	recorder *_MockServicesRecorder
}

// Recorder for MockServices (not exported)
type _MockServicesRecorder struct {
	mock *MockServices
}

func NewMockServices(ctrl *gomock.Controller) *MockServices {
	mock := &MockServices{ctrl: ctrl}
	mock.recorder = &_MockServicesRecorder{mock}
	return mock
}

func (_m *MockServices) EXPECT() *_MockServicesRecorder {
	return _m.recorder
}

func (_m *MockServices) Advertise(ad Advertisement) error {
	ret := _m.ctrl.Call(_m, "Advertise", ad)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockServicesRecorder) Advertise(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Advertise", arg0)
}

func (_m *MockServices) Unadvertise(service ServiceID, id string) error {
	ret := _m.ctrl.Call(_m, "Unadvertise", service, id)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockServicesRecorder) Unadvertise(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Unadvertise", arg0, arg1)
}

func (_m *MockServices) Query(service ServiceID, opts QueryOptions) (Service, error) {
	ret := _m.ctrl.Call(_m, "Query", service, opts)
	ret0, _ := ret[0].(Service)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockServicesRecorder) Query(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Query", arg0, arg1)
}

func (_m *MockServices) Watch(service ServiceID, opts QueryOptions) (Watch, error) {
	ret := _m.ctrl.Call(_m, "Watch", service, opts)
	ret0, _ := ret[0].(Watch)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockServicesRecorder) Watch(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Watch", arg0, arg1)
}

func (_m *MockServices) Metadata(sid ServiceID) (Metadata, error) {
	ret := _m.ctrl.Call(_m, "Metadata", sid)
	ret0, _ := ret[0].(Metadata)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockServicesRecorder) Metadata(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Metadata", arg0)
}

func (_m *MockServices) SetMetadata(sid ServiceID, m Metadata) error {
	ret := _m.ctrl.Call(_m, "SetMetadata", sid, m)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockServicesRecorder) SetMetadata(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "SetMetadata", arg0, arg1)
}

func (_m *MockServices) PlacementService(sid ServiceID, popts placement.Options) (placement.Service, error) {
	ret := _m.ctrl.Call(_m, "PlacementService", sid, popts)
	ret0, _ := ret[0].(placement.Service)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockServicesRecorder) PlacementService(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "PlacementService", arg0, arg1)
}

func (_m *MockServices) HeartbeatService(service ServiceID) (HeartbeatService, error) {
	ret := _m.ctrl.Call(_m, "HeartbeatService", service)
	ret0, _ := ret[0].(HeartbeatService)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockServicesRecorder) HeartbeatService(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "HeartbeatService", arg0)
}

func (_m *MockServices) LeaderService(service ServiceID, opts ElectionOptions) (LeaderService, error) {
	ret := _m.ctrl.Call(_m, "LeaderService", service, opts)
	ret0, _ := ret[0].(LeaderService)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockServicesRecorder) LeaderService(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "LeaderService", arg0, arg1)
}

// Mock of Watch interface
type MockWatch struct {
	ctrl     *gomock.Controller
	recorder *_MockWatchRecorder
}

// Recorder for MockWatch (not exported)
type _MockWatchRecorder struct {
	mock *MockWatch
}

func NewMockWatch(ctrl *gomock.Controller) *MockWatch {
	mock := &MockWatch{ctrl: ctrl}
	mock.recorder = &_MockWatchRecorder{mock}
	return mock
}

func (_m *MockWatch) EXPECT() *_MockWatchRecorder {
	return _m.recorder
}

func (_m *MockWatch) Close() {
	_m.ctrl.Call(_m, "Close")
}

func (_mr *_MockWatchRecorder) Close() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Close")
}

func (_m *MockWatch) C() <-chan struct{} {
	ret := _m.ctrl.Call(_m, "C")
	ret0, _ := ret[0].(<-chan struct{})
	return ret0
}

func (_mr *_MockWatchRecorder) C() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "C")
}

func (_m *MockWatch) Get() Service {
	ret := _m.ctrl.Call(_m, "Get")
	ret0, _ := ret[0].(Service)
	return ret0
}

func (_mr *_MockWatchRecorder) Get() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Get")
}

// Mock of Service interface
type MockService struct {
	ctrl     *gomock.Controller
	recorder *_MockServiceRecorder
}

// Recorder for MockService (not exported)
type _MockServiceRecorder struct {
	mock *MockService
}

func NewMockService(ctrl *gomock.Controller) *MockService {
	mock := &MockService{ctrl: ctrl}
	mock.recorder = &_MockServiceRecorder{mock}
	return mock
}

func (_m *MockService) EXPECT() *_MockServiceRecorder {
	return _m.recorder
}

func (_m *MockService) Instance(instanceID string) (ServiceInstance, error) {
	ret := _m.ctrl.Call(_m, "Instance", instanceID)
	ret0, _ := ret[0].(ServiceInstance)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockServiceRecorder) Instance(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Instance", arg0)
}

func (_m *MockService) Instances() []ServiceInstance {
	ret := _m.ctrl.Call(_m, "Instances")
	ret0, _ := ret[0].([]ServiceInstance)
	return ret0
}

func (_mr *_MockServiceRecorder) Instances() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Instances")
}

func (_m *MockService) SetInstances(insts []ServiceInstance) Service {
	ret := _m.ctrl.Call(_m, "SetInstances", insts)
	ret0, _ := ret[0].(Service)
	return ret0
}

func (_mr *_MockServiceRecorder) SetInstances(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "SetInstances", arg0)
}

func (_m *MockService) Replication() ServiceReplication {
	ret := _m.ctrl.Call(_m, "Replication")
	ret0, _ := ret[0].(ServiceReplication)
	return ret0
}

func (_mr *_MockServiceRecorder) Replication() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Replication")
}

func (_m *MockService) SetReplication(r ServiceReplication) Service {
	ret := _m.ctrl.Call(_m, "SetReplication", r)
	ret0, _ := ret[0].(Service)
	return ret0
}

func (_mr *_MockServiceRecorder) SetReplication(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "SetReplication", arg0)
}

func (_m *MockService) Sharding() ServiceSharding {
	ret := _m.ctrl.Call(_m, "Sharding")
	ret0, _ := ret[0].(ServiceSharding)
	return ret0
}

func (_mr *_MockServiceRecorder) Sharding() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Sharding")
}

func (_m *MockService) SetSharding(s ServiceSharding) Service {
	ret := _m.ctrl.Call(_m, "SetSharding", s)
	ret0, _ := ret[0].(Service)
	return ret0
}

func (_mr *_MockServiceRecorder) SetSharding(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "SetSharding", arg0)
}

// Mock of ServiceReplication interface
type MockServiceReplication struct {
	ctrl     *gomock.Controller
	recorder *_MockServiceReplicationRecorder
}

// Recorder for MockServiceReplication (not exported)
type _MockServiceReplicationRecorder struct {
	mock *MockServiceReplication
}

func NewMockServiceReplication(ctrl *gomock.Controller) *MockServiceReplication {
	mock := &MockServiceReplication{ctrl: ctrl}
	mock.recorder = &_MockServiceReplicationRecorder{mock}
	return mock
}

func (_m *MockServiceReplication) EXPECT() *_MockServiceReplicationRecorder {
	return _m.recorder
}

func (_m *MockServiceReplication) Replicas() int {
	ret := _m.ctrl.Call(_m, "Replicas")
	ret0, _ := ret[0].(int)
	return ret0
}

func (_mr *_MockServiceReplicationRecorder) Replicas() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Replicas")
}

func (_m *MockServiceReplication) SetReplicas(r int) ServiceReplication {
	ret := _m.ctrl.Call(_m, "SetReplicas", r)
	ret0, _ := ret[0].(ServiceReplication)
	return ret0
}

func (_mr *_MockServiceReplicationRecorder) SetReplicas(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "SetReplicas", arg0)
}

// Mock of ServiceSharding interface
type MockServiceSharding struct {
	ctrl     *gomock.Controller
	recorder *_MockServiceShardingRecorder
}

// Recorder for MockServiceSharding (not exported)
type _MockServiceShardingRecorder struct {
	mock *MockServiceSharding
}

func NewMockServiceSharding(ctrl *gomock.Controller) *MockServiceSharding {
	mock := &MockServiceSharding{ctrl: ctrl}
	mock.recorder = &_MockServiceShardingRecorder{mock}
	return mock
}

func (_m *MockServiceSharding) EXPECT() *_MockServiceShardingRecorder {
	return _m.recorder
}

func (_m *MockServiceSharding) NumShards() int {
	ret := _m.ctrl.Call(_m, "NumShards")
	ret0, _ := ret[0].(int)
	return ret0
}

func (_mr *_MockServiceShardingRecorder) NumShards() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "NumShards")
}

func (_m *MockServiceSharding) SetNumShards(n int) ServiceSharding {
	ret := _m.ctrl.Call(_m, "SetNumShards", n)
	ret0, _ := ret[0].(ServiceSharding)
	return ret0
}

func (_mr *_MockServiceShardingRecorder) SetNumShards(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "SetNumShards", arg0)
}

func (_m *MockServiceSharding) IsSharded() bool {
	ret := _m.ctrl.Call(_m, "IsSharded")
	ret0, _ := ret[0].(bool)
	return ret0
}

func (_mr *_MockServiceShardingRecorder) IsSharded() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "IsSharded")
}

func (_m *MockServiceSharding) SetIsSharded(s bool) ServiceSharding {
	ret := _m.ctrl.Call(_m, "SetIsSharded", s)
	ret0, _ := ret[0].(ServiceSharding)
	return ret0
}

func (_mr *_MockServiceShardingRecorder) SetIsSharded(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "SetIsSharded", arg0)
}

// Mock of ServiceInstance interface
type MockServiceInstance struct {
	ctrl     *gomock.Controller
	recorder *_MockServiceInstanceRecorder
}

// Recorder for MockServiceInstance (not exported)
type _MockServiceInstanceRecorder struct {
	mock *MockServiceInstance
}

func NewMockServiceInstance(ctrl *gomock.Controller) *MockServiceInstance {
	mock := &MockServiceInstance{ctrl: ctrl}
	mock.recorder = &_MockServiceInstanceRecorder{mock}
	return mock
}

func (_m *MockServiceInstance) EXPECT() *_MockServiceInstanceRecorder {
	return _m.recorder
}

func (_m *MockServiceInstance) ServiceID() ServiceID {
	ret := _m.ctrl.Call(_m, "ServiceID")
	ret0, _ := ret[0].(ServiceID)
	return ret0
}

func (_mr *_MockServiceInstanceRecorder) ServiceID() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "ServiceID")
}

func (_m *MockServiceInstance) SetServiceID(service ServiceID) ServiceInstance {
	ret := _m.ctrl.Call(_m, "SetServiceID", service)
	ret0, _ := ret[0].(ServiceInstance)
	return ret0
}

func (_mr *_MockServiceInstanceRecorder) SetServiceID(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "SetServiceID", arg0)
}

func (_m *MockServiceInstance) InstanceID() string {
	ret := _m.ctrl.Call(_m, "InstanceID")
	ret0, _ := ret[0].(string)
	return ret0
}

func (_mr *_MockServiceInstanceRecorder) InstanceID() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "InstanceID")
}

func (_m *MockServiceInstance) SetInstanceID(id string) ServiceInstance {
	ret := _m.ctrl.Call(_m, "SetInstanceID", id)
	ret0, _ := ret[0].(ServiceInstance)
	return ret0
}

func (_mr *_MockServiceInstanceRecorder) SetInstanceID(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "SetInstanceID", arg0)
}

func (_m *MockServiceInstance) Endpoint() string {
	ret := _m.ctrl.Call(_m, "Endpoint")
	ret0, _ := ret[0].(string)
	return ret0
}

func (_mr *_MockServiceInstanceRecorder) Endpoint() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Endpoint")
}

func (_m *MockServiceInstance) SetEndpoint(e string) ServiceInstance {
	ret := _m.ctrl.Call(_m, "SetEndpoint", e)
	ret0, _ := ret[0].(ServiceInstance)
	return ret0
}

func (_mr *_MockServiceInstanceRecorder) SetEndpoint(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "SetEndpoint", arg0)
}

func (_m *MockServiceInstance) Shards() shard.Shards {
	ret := _m.ctrl.Call(_m, "Shards")
	ret0, _ := ret[0].(shard.Shards)
	return ret0
}

func (_mr *_MockServiceInstanceRecorder) Shards() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Shards")
}

func (_m *MockServiceInstance) SetShards(s shard.Shards) ServiceInstance {
	ret := _m.ctrl.Call(_m, "SetShards", s)
	ret0, _ := ret[0].(ServiceInstance)
	return ret0
}

func (_mr *_MockServiceInstanceRecorder) SetShards(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "SetShards", arg0)
}

// Mock of Advertisement interface
type MockAdvertisement struct {
	ctrl     *gomock.Controller
	recorder *_MockAdvertisementRecorder
}

// Recorder for MockAdvertisement (not exported)
type _MockAdvertisementRecorder struct {
	mock *MockAdvertisement
}

func NewMockAdvertisement(ctrl *gomock.Controller) *MockAdvertisement {
	mock := &MockAdvertisement{ctrl: ctrl}
	mock.recorder = &_MockAdvertisementRecorder{mock}
	return mock
}

func (_m *MockAdvertisement) EXPECT() *_MockAdvertisementRecorder {
	return _m.recorder
}

func (_m *MockAdvertisement) ServiceID() ServiceID {
	ret := _m.ctrl.Call(_m, "ServiceID")
	ret0, _ := ret[0].(ServiceID)
	return ret0
}

func (_mr *_MockAdvertisementRecorder) ServiceID() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "ServiceID")
}

func (_m *MockAdvertisement) SetServiceID(service ServiceID) Advertisement {
	ret := _m.ctrl.Call(_m, "SetServiceID", service)
	ret0, _ := ret[0].(Advertisement)
	return ret0
}

func (_mr *_MockAdvertisementRecorder) SetServiceID(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "SetServiceID", arg0)
}

func (_m *MockAdvertisement) Health() func() error {
	ret := _m.ctrl.Call(_m, "Health")
	ret0, _ := ret[0].(func() error)
	return ret0
}

func (_mr *_MockAdvertisementRecorder) Health() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Health")
}

func (_m *MockAdvertisement) SetHealth(health func() error) Advertisement {
	ret := _m.ctrl.Call(_m, "SetHealth", health)
	ret0, _ := ret[0].(Advertisement)
	return ret0
}

func (_mr *_MockAdvertisementRecorder) SetHealth(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "SetHealth", arg0)
}

func (_m *MockAdvertisement) PlacementInstance() placement.Instance {
	ret := _m.ctrl.Call(_m, "PlacementInstance")
	ret0, _ := ret[0].(placement.Instance)
	return ret0
}

func (_mr *_MockAdvertisementRecorder) PlacementInstance() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "PlacementInstance")
}

func (_m *MockAdvertisement) SetPlacementInstance(p placement.Instance) Advertisement {
	ret := _m.ctrl.Call(_m, "SetPlacementInstance", p)
	ret0, _ := ret[0].(Advertisement)
	return ret0
}

func (_mr *_MockAdvertisementRecorder) SetPlacementInstance(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "SetPlacementInstance", arg0)
}

// Mock of ServiceID interface
type MockServiceID struct {
	ctrl     *gomock.Controller
	recorder *_MockServiceIDRecorder
}

// Recorder for MockServiceID (not exported)
type _MockServiceIDRecorder struct {
	mock *MockServiceID
}

func NewMockServiceID(ctrl *gomock.Controller) *MockServiceID {
	mock := &MockServiceID{ctrl: ctrl}
	mock.recorder = &_MockServiceIDRecorder{mock}
	return mock
}

func (_m *MockServiceID) EXPECT() *_MockServiceIDRecorder {
	return _m.recorder
}

func (_m *MockServiceID) Name() string {
	ret := _m.ctrl.Call(_m, "Name")
	ret0, _ := ret[0].(string)
	return ret0
}

func (_mr *_MockServiceIDRecorder) Name() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Name")
}

func (_m *MockServiceID) SetName(s string) ServiceID {
	ret := _m.ctrl.Call(_m, "SetName", s)
	ret0, _ := ret[0].(ServiceID)
	return ret0
}

func (_mr *_MockServiceIDRecorder) SetName(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "SetName", arg0)
}

func (_m *MockServiceID) Environment() string {
	ret := _m.ctrl.Call(_m, "Environment")
	ret0, _ := ret[0].(string)
	return ret0
}

func (_mr *_MockServiceIDRecorder) Environment() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Environment")
}

func (_m *MockServiceID) SetEnvironment(env string) ServiceID {
	ret := _m.ctrl.Call(_m, "SetEnvironment", env)
	ret0, _ := ret[0].(ServiceID)
	return ret0
}

func (_mr *_MockServiceIDRecorder) SetEnvironment(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "SetEnvironment", arg0)
}

func (_m *MockServiceID) Zone() string {
	ret := _m.ctrl.Call(_m, "Zone")
	ret0, _ := ret[0].(string)
	return ret0
}

func (_mr *_MockServiceIDRecorder) Zone() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Zone")
}

func (_m *MockServiceID) SetZone(zone string) ServiceID {
	ret := _m.ctrl.Call(_m, "SetZone", zone)
	ret0, _ := ret[0].(ServiceID)
	return ret0
}

func (_mr *_MockServiceIDRecorder) SetZone(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "SetZone", arg0)
}

func (_m *MockServiceID) String() string {
	ret := _m.ctrl.Call(_m, "String")
	ret0, _ := ret[0].(string)
	return ret0
}

func (_mr *_MockServiceIDRecorder) String() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "String")
}

// Mock of QueryOptions interface
type MockQueryOptions struct {
	ctrl     *gomock.Controller
	recorder *_MockQueryOptionsRecorder
}

// Recorder for MockQueryOptions (not exported)
type _MockQueryOptionsRecorder struct {
	mock *MockQueryOptions
}

func NewMockQueryOptions(ctrl *gomock.Controller) *MockQueryOptions {
	mock := &MockQueryOptions{ctrl: ctrl}
	mock.recorder = &_MockQueryOptionsRecorder{mock}
	return mock
}

func (_m *MockQueryOptions) EXPECT() *_MockQueryOptionsRecorder {
	return _m.recorder
}

func (_m *MockQueryOptions) IncludeUnhealthy() bool {
	ret := _m.ctrl.Call(_m, "IncludeUnhealthy")
	ret0, _ := ret[0].(bool)
	return ret0
}

func (_mr *_MockQueryOptionsRecorder) IncludeUnhealthy() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "IncludeUnhealthy")
}

func (_m *MockQueryOptions) SetIncludeUnhealthy(h bool) QueryOptions {
	ret := _m.ctrl.Call(_m, "SetIncludeUnhealthy", h)
	ret0, _ := ret[0].(QueryOptions)
	return ret0
}

func (_mr *_MockQueryOptionsRecorder) SetIncludeUnhealthy(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "SetIncludeUnhealthy", arg0)
}

// Mock of Metadata interface
type MockMetadata struct {
	ctrl     *gomock.Controller
	recorder *_MockMetadataRecorder
}

// Recorder for MockMetadata (not exported)
type _MockMetadataRecorder struct {
	mock *MockMetadata
}

func NewMockMetadata(ctrl *gomock.Controller) *MockMetadata {
	mock := &MockMetadata{ctrl: ctrl}
	mock.recorder = &_MockMetadataRecorder{mock}
	return mock
}

func (_m *MockMetadata) EXPECT() *_MockMetadataRecorder {
	return _m.recorder
}

func (_m *MockMetadata) String() string {
	ret := _m.ctrl.Call(_m, "String")
	ret0, _ := ret[0].(string)
	return ret0
}

func (_mr *_MockMetadataRecorder) String() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "String")
}

func (_m *MockMetadata) Port() uint32 {
	ret := _m.ctrl.Call(_m, "Port")
	ret0, _ := ret[0].(uint32)
	return ret0
}

func (_mr *_MockMetadataRecorder) Port() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Port")
}

func (_m *MockMetadata) SetPort(p uint32) Metadata {
	ret := _m.ctrl.Call(_m, "SetPort", p)
	ret0, _ := ret[0].(Metadata)
	return ret0
}

func (_mr *_MockMetadataRecorder) SetPort(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "SetPort", arg0)
}

func (_m *MockMetadata) LivenessInterval() time.Duration {
	ret := _m.ctrl.Call(_m, "LivenessInterval")
	ret0, _ := ret[0].(time.Duration)
	return ret0
}

func (_mr *_MockMetadataRecorder) LivenessInterval() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "LivenessInterval")
}

func (_m *MockMetadata) SetLivenessInterval(l time.Duration) Metadata {
	ret := _m.ctrl.Call(_m, "SetLivenessInterval", l)
	ret0, _ := ret[0].(Metadata)
	return ret0
}

func (_mr *_MockMetadataRecorder) SetLivenessInterval(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "SetLivenessInterval", arg0)
}

func (_m *MockMetadata) HeartbeatInterval() time.Duration {
	ret := _m.ctrl.Call(_m, "HeartbeatInterval")
	ret0, _ := ret[0].(time.Duration)
	return ret0
}

func (_mr *_MockMetadataRecorder) HeartbeatInterval() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "HeartbeatInterval")
}

func (_m *MockMetadata) SetHeartbeatInterval(h time.Duration) Metadata {
	ret := _m.ctrl.Call(_m, "SetHeartbeatInterval", h)
	ret0, _ := ret[0].(Metadata)
	return ret0
}

func (_mr *_MockMetadataRecorder) SetHeartbeatInterval(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "SetHeartbeatInterval", arg0)
}

func (_m *MockMetadata) Proto() (*metadatapb.Metadata, error) {
	ret := _m.ctrl.Call(_m, "Proto")
	ret0, _ := ret[0].(*metadatapb.Metadata)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockMetadataRecorder) Proto() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Proto")
}

// Mock of HeartbeatService interface
type MockHeartbeatService struct {
	ctrl     *gomock.Controller
	recorder *_MockHeartbeatServiceRecorder
}

// Recorder for MockHeartbeatService (not exported)
type _MockHeartbeatServiceRecorder struct {
	mock *MockHeartbeatService
}

func NewMockHeartbeatService(ctrl *gomock.Controller) *MockHeartbeatService {
	mock := &MockHeartbeatService{ctrl: ctrl}
	mock.recorder = &_MockHeartbeatServiceRecorder{mock}
	return mock
}

func (_m *MockHeartbeatService) EXPECT() *_MockHeartbeatServiceRecorder {
	return _m.recorder
}

func (_m *MockHeartbeatService) Heartbeat(instance placement.Instance, ttl time.Duration) error {
	ret := _m.ctrl.Call(_m, "Heartbeat", instance, ttl)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockHeartbeatServiceRecorder) Heartbeat(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Heartbeat", arg0, arg1)
}

func (_m *MockHeartbeatService) Get() ([]string, error) {
	ret := _m.ctrl.Call(_m, "Get")
	ret0, _ := ret[0].([]string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockHeartbeatServiceRecorder) Get() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Get")
}

func (_m *MockHeartbeatService) GetInstances() ([]placement.Instance, error) {
	ret := _m.ctrl.Call(_m, "GetInstances")
	ret0, _ := ret[0].([]placement.Instance)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockHeartbeatServiceRecorder) GetInstances() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "GetInstances")
}

func (_m *MockHeartbeatService) Delete(instance string) error {
	ret := _m.ctrl.Call(_m, "Delete", instance)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockHeartbeatServiceRecorder) Delete(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Delete", arg0)
}

func (_m *MockHeartbeatService) Watch() (watch.Watch, error) {
	ret := _m.ctrl.Call(_m, "Watch")
	ret0, _ := ret[0].(watch.Watch)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockHeartbeatServiceRecorder) Watch() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Watch")
}

// Mock of ElectionOptions interface
type MockElectionOptions struct {
	ctrl     *gomock.Controller
	recorder *_MockElectionOptionsRecorder
}

// Recorder for MockElectionOptions (not exported)
type _MockElectionOptionsRecorder struct {
	mock *MockElectionOptions
}

func NewMockElectionOptions(ctrl *gomock.Controller) *MockElectionOptions {
	mock := &MockElectionOptions{ctrl: ctrl}
	mock.recorder = &_MockElectionOptionsRecorder{mock}
	return mock
}

func (_m *MockElectionOptions) EXPECT() *_MockElectionOptionsRecorder {
	return _m.recorder
}

func (_m *MockElectionOptions) LeaderTimeout() time.Duration {
	ret := _m.ctrl.Call(_m, "LeaderTimeout")
	ret0, _ := ret[0].(time.Duration)
	return ret0
}

func (_mr *_MockElectionOptionsRecorder) LeaderTimeout() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "LeaderTimeout")
}

func (_m *MockElectionOptions) SetLeaderTimeout(t time.Duration) ElectionOptions {
	ret := _m.ctrl.Call(_m, "SetLeaderTimeout", t)
	ret0, _ := ret[0].(ElectionOptions)
	return ret0
}

func (_mr *_MockElectionOptionsRecorder) SetLeaderTimeout(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "SetLeaderTimeout", arg0)
}

func (_m *MockElectionOptions) ResignTimeout() time.Duration {
	ret := _m.ctrl.Call(_m, "ResignTimeout")
	ret0, _ := ret[0].(time.Duration)
	return ret0
}

func (_mr *_MockElectionOptionsRecorder) ResignTimeout() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "ResignTimeout")
}

func (_m *MockElectionOptions) SetResignTimeout(t time.Duration) ElectionOptions {
	ret := _m.ctrl.Call(_m, "SetResignTimeout", t)
	ret0, _ := ret[0].(ElectionOptions)
	return ret0
}

func (_mr *_MockElectionOptionsRecorder) SetResignTimeout(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "SetResignTimeout", arg0)
}

func (_m *MockElectionOptions) TTLSecs() int {
	ret := _m.ctrl.Call(_m, "TTLSecs")
	ret0, _ := ret[0].(int)
	return ret0
}

func (_mr *_MockElectionOptionsRecorder) TTLSecs() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "TTLSecs")
}

func (_m *MockElectionOptions) SetTTLSecs(ttl int) ElectionOptions {
	ret := _m.ctrl.Call(_m, "SetTTLSecs", ttl)
	ret0, _ := ret[0].(ElectionOptions)
	return ret0
}

func (_mr *_MockElectionOptionsRecorder) SetTTLSecs(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "SetTTLSecs", arg0)
}

// Mock of CampaignOptions interface
type MockCampaignOptions struct {
	ctrl     *gomock.Controller
	recorder *_MockCampaignOptionsRecorder
}

// Recorder for MockCampaignOptions (not exported)
type _MockCampaignOptionsRecorder struct {
	mock *MockCampaignOptions
}

func NewMockCampaignOptions(ctrl *gomock.Controller) *MockCampaignOptions {
	mock := &MockCampaignOptions{ctrl: ctrl}
	mock.recorder = &_MockCampaignOptionsRecorder{mock}
	return mock
}

func (_m *MockCampaignOptions) EXPECT() *_MockCampaignOptionsRecorder {
	return _m.recorder
}

func (_m *MockCampaignOptions) LeaderValue() string {
	ret := _m.ctrl.Call(_m, "LeaderValue")
	ret0, _ := ret[0].(string)
	return ret0
}

func (_mr *_MockCampaignOptionsRecorder) LeaderValue() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "LeaderValue")
}

func (_m *MockCampaignOptions) SetLeaderValue(v string) CampaignOptions {
	ret := _m.ctrl.Call(_m, "SetLeaderValue", v)
	ret0, _ := ret[0].(CampaignOptions)
	return ret0
}

func (_mr *_MockCampaignOptionsRecorder) SetLeaderValue(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "SetLeaderValue", arg0)
}

// Mock of LeaderService interface
type MockLeaderService struct {
	ctrl     *gomock.Controller
	recorder *_MockLeaderServiceRecorder
}

// Recorder for MockLeaderService (not exported)
type _MockLeaderServiceRecorder struct {
	mock *MockLeaderService
}

func NewMockLeaderService(ctrl *gomock.Controller) *MockLeaderService {
	mock := &MockLeaderService{ctrl: ctrl}
	mock.recorder = &_MockLeaderServiceRecorder{mock}
	return mock
}

func (_m *MockLeaderService) EXPECT() *_MockLeaderServiceRecorder {
	return _m.recorder
}

func (_m *MockLeaderService) Close() error {
	ret := _m.ctrl.Call(_m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockLeaderServiceRecorder) Close() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Close")
}

func (_m *MockLeaderService) Campaign(electionID string, opts CampaignOptions) (<-chan campaign.Status, error) {
	ret := _m.ctrl.Call(_m, "Campaign", electionID, opts)
	ret0, _ := ret[0].(<-chan campaign.Status)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockLeaderServiceRecorder) Campaign(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Campaign", arg0, arg1)
}

func (_m *MockLeaderService) Resign(electionID string) error {
	ret := _m.ctrl.Call(_m, "Resign", electionID)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockLeaderServiceRecorder) Resign(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Resign", arg0)
}

func (_m *MockLeaderService) Leader(electionID string) (string, error) {
	ret := _m.ctrl.Call(_m, "Leader", electionID)
	ret0, _ := ret[0].(string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockLeaderServiceRecorder) Leader(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Leader", arg0)
}
