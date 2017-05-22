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

	// TTL for leadership lease.
	SetTTL(seconds int) Options
	TTL() int

	Validate() error
}

// NewOptions returns an instance of leader options.
func NewOptions() Options {
	return options{}
}

type options struct {
	sid services.ServiceID
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

func (o options) TTL() int {
	return o.ttl
}

func (o options) SetTTL(seconds int) Options {
	o.ttl = seconds
	return o
}

func (o options) Validate() error {
	if o.sid == nil {
		return errors.New("leader options must specify service ID")
	}

	return nil
}
