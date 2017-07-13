package leader

import (
	"errors"

	"github.com/m3db/m3cluster/services"
)

var (
	errMissingSid   = errors.New("leader options must specify service ID")
	errMissingEOpts = errors.New("leader options election opts cannot be nil")
)

// Options describe options for creating a leader service.
type Options interface {
	// Service the election is campaigning for.
	ServiceID() services.ServiceID
	SetServiceID(sid services.ServiceID) Options

	ElectionOpts() services.ElectionOptions
	SetElectionOpts(e services.ElectionOptions) Options

	Validate() error
}

// NewOptions returns an instance of leader options.
func NewOptions() Options {
	return options{
		eo: services.NewElectionOptions(),
	}
}

type options struct {
	sid services.ServiceID
	eo  services.ElectionOptions
}

func (o options) ServiceID() services.ServiceID {
	return o.sid
}

func (o options) SetServiceID(sid services.ServiceID) Options {
	o.sid = sid
	return o
}

func (o options) ElectionOpts() services.ElectionOptions {
	return o.eo
}

func (o options) SetElectionOpts(eo services.ElectionOptions) Options {
	o.eo = eo
	return o
}

func (o options) Validate() error {
	if o.sid == nil {
		return errMissingSid
	}

	// This shouldn't happen since we have sane defaults but prevents user error.
	if o.eo == nil {
		return errMissingEOpts
	}

	return nil
}
