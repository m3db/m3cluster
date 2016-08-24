// Automatically generated by MockGen. DO NOT EDIT!
// Source: /Users/chaowang/gocode/src/github.com/m3db/m3cluster/services/services.go

package services

import (
	gomock "github.com/golang/mock/gomock"
	m3cluster "github.com/m3db/m3cluster"
	watch "github.com/m3db/m3x/watch"
)

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

func (_m *MockServiceInstance) Service() string {
	ret := _m.ctrl.Call(_m, "Service")
	ret0, _ := ret[0].(string)
	return ret0
}

func (_mr *_MockServiceInstanceRecorder) Service() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Service")
}

func (_m *MockServiceInstance) SetService(s string) ServiceInstance {
	ret := _m.ctrl.Call(_m, "SetService", s)
	ret0, _ := ret[0].(ServiceInstance)
	return ret0
}

func (_mr *_MockServiceInstanceRecorder) SetService(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "SetService", arg0)
}

func (_m *MockServiceInstance) ID() string {
	ret := _m.ctrl.Call(_m, "ID")
	ret0, _ := ret[0].(string)
	return ret0
}

func (_mr *_MockServiceInstanceRecorder) ID() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "ID")
}

func (_m *MockServiceInstance) SetID(id string) ServiceInstance {
	ret := _m.ctrl.Call(_m, "SetID", id)
	ret0, _ := ret[0].(ServiceInstance)
	return ret0
}

func (_mr *_MockServiceInstanceRecorder) SetID(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "SetID", arg0)
}

func (_m *MockServiceInstance) Zone() string {
	ret := _m.ctrl.Call(_m, "Zone")
	ret0, _ := ret[0].(string)
	return ret0
}

func (_mr *_MockServiceInstanceRecorder) Zone() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Zone")
}

func (_m *MockServiceInstance) SetZone(z string) ServiceInstance {
	ret := _m.ctrl.Call(_m, "SetZone", z)
	ret0, _ := ret[0].(ServiceInstance)
	return ret0
}

func (_mr *_MockServiceInstanceRecorder) SetZone(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "SetZone", arg0)
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

func (_m *MockServiceInstance) Shards() m3cluster.Shards {
	ret := _m.ctrl.Call(_m, "Shards")
	ret0, _ := ret[0].(m3cluster.Shards)
	return ret0
}

func (_mr *_MockServiceInstanceRecorder) Shards() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Shards")
}

func (_m *MockServiceInstance) SetShards(s m3cluster.Shards) ServiceInstance {
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

func (_m *MockAdvertisement) ID() string {
	ret := _m.ctrl.Call(_m, "ID")
	ret0, _ := ret[0].(string)
	return ret0
}

func (_mr *_MockAdvertisementRecorder) ID() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "ID")
}

func (_m *MockAdvertisement) SetID(id string) Advertisement {
	ret := _m.ctrl.Call(_m, "SetID", id)
	ret0, _ := ret[0].(Advertisement)
	return ret0
}

func (_mr *_MockAdvertisementRecorder) SetID(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "SetID", arg0)
}

func (_m *MockAdvertisement) Service() string {
	ret := _m.ctrl.Call(_m, "Service")
	ret0, _ := ret[0].(string)
	return ret0
}

func (_mr *_MockAdvertisementRecorder) Service() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Service")
}

func (_m *MockAdvertisement) SetService(service string) Advertisement {
	ret := _m.ctrl.Call(_m, "SetService", service)
	ret0, _ := ret[0].(Advertisement)
	return ret0
}

func (_mr *_MockAdvertisementRecorder) SetService(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "SetService", arg0)
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

func (_m *MockAdvertisement) Endpoint() string {
	ret := _m.ctrl.Call(_m, "Endpoint")
	ret0, _ := ret[0].(string)
	return ret0
}

func (_mr *_MockAdvertisementRecorder) Endpoint() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Endpoint")
}

func (_m *MockAdvertisement) SetEndpoint(e string) Advertisement {
	ret := _m.ctrl.Call(_m, "SetEndpoint", e)
	ret0, _ := ret[0].(Advertisement)
	return ret0
}

func (_mr *_MockAdvertisementRecorder) SetEndpoint(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "SetEndpoint", arg0)
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

func (_m *MockQueryOptions) Zones() []string {
	ret := _m.ctrl.Call(_m, "Zones")
	ret0, _ := ret[0].([]string)
	return ret0
}

func (_mr *_MockQueryOptionsRecorder) Zones() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Zones")
}

func (_m *MockQueryOptions) SetZones(zones []string) QueryOptions {
	ret := _m.ctrl.Call(_m, "SetZones", zones)
	ret0, _ := ret[0].(QueryOptions)
	return ret0
}

func (_mr *_MockQueryOptionsRecorder) SetZones(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "SetZones", arg0)
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

func (_m *MockServices) Unadvertise(service string, id string) error {
	ret := _m.ctrl.Call(_m, "Unadvertise", service, id)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockServicesRecorder) Unadvertise(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Unadvertise", arg0, arg1)
}

func (_m *MockServices) QueryInstances(service string, opts QueryOptions) ([]ServiceInstance, error) {
	ret := _m.ctrl.Call(_m, "QueryInstances", service, opts)
	ret0, _ := ret[0].([]ServiceInstance)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockServicesRecorder) QueryInstances(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "QueryInstances", arg0, arg1)
}

func (_m *MockServices) WatchInstances(service string, opts QueryOptions) (watch.Watch, error) {
	ret := _m.ctrl.Call(_m, "WatchInstances", service, opts)
	ret0, _ := ret[0].(watch.Watch)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockServicesRecorder) WatchInstances(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "WatchInstances", arg0, arg1)
}
