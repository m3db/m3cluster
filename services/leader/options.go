package leader

import (
	"errors"
	"os"

	"github.com/m3db/m3cluster/services"
)

// Options describe options for creating a leader service.
type Options interface {
	// Service the election is campaigning for.
	ServiceID() services.ServiceID
	SetServiceID(sid services.ServiceID) Options

	// Override value to propose in election. If empty, the hostname of the
	// caller is returned (which should be suitable for most cases).
	OverrideValue() string
	SetOverrideValue(v string) Options

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
	val string
	ttl int
}

func (o options) ServiceID() services.ServiceID {
	return o.sid
}

func (o options) SetServiceID(sid services.ServiceID) Options {
	o.sid = sid
	return o
}

func (o options) OverrideValue() string {
	if o.val != "" {
		return o.val
	}

	h, err := os.Hostname()
	if err != nil {
		return defaultHostname
	}
	return h
}

func (o options) SetOverrideValue(v string) Options {
	o.val = v
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
		return errors.New("leader options must specify service ID")
	}

	// this shouldn't happen since we have sane defaults but prevents user error
	if o.eo == nil {
		return errors.New("leader options election opts cannot be nil")
	}

	return nil
}
