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

package changeset

import (
	"errors"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/m3db/m3cluster/generated/proto/changesetpb"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3x/log"
)

var (
	// ErrAlreadyCommitted is returned when attempting to commit an already
	// committed ChangeSet
	ErrAlreadyCommitted = errors.New("change list already committed")

	// ErrCommitInProgress is returned when attempting to make a change while a
	// commit is in progress
	ErrCommitInProgress = errors.New("commit in progress")

	// ErrUnknownVersion is returned when attempting to commit a change for
	// a version that doesn't exist
	ErrUnknownVersion = errors.New("unknown version")
)

// ManagerOptions are options used in creating a new ChangeSet Manager
type ManagerOptions interface {
	// KV is the KVStore holding the configuration
	KV(kv kv.Store) ManagerOptions
	GetKV() kv.Store

	// ConfigKey is the key holding the configuration object
	ConfigKey(key string) ManagerOptions
	GetConfigKey() string

	// Logger is the logger to use
	Logger(logger xlog.Logger) ManagerOptions
	GetLogger() xlog.Logger

	// ConfigType is a proto.Message defining the structure of the configuration
	// object.  Clones of this proto will be used to unmarshal configuration
	// instances
	ConfigType(config proto.Message) ManagerOptions
	GetConfigType() proto.Message

	// ChangesType is a proto.Message defining the structure of the changes
	// object.  Clones of this protol will be used to unmarshal change list
	// instances.
	ChangesType(changes proto.Message) ManagerOptions
	GetChangesType() proto.Message
}

// NewManagerOptions creates an empty ManagerOptions
func NewManagerOptions() ManagerOptions { return new(managerOptions) }

// A ChangeFn adds a change to an existing set of changes
type ChangeFn func(config, changes proto.Message) error

// An ApplyFn applies a set of changes to a configuration, resulting in a new
// configuration
type ApplyFn func(config, changes proto.Message) error

// A Manager manages sets of changes in a version friendly mannger
type Manager interface {
	// Change creates a new change against the latest configuration, adding it
	// to the set of pending changes for that configuration
	Change(change ChangeFn) error

	// GetLastUncommitted gets the latest configuration and the uncommitted
	// changes associated with that config version
	//	GetLastUncommitted(config, changes proto.Message) (int, error)

	// Commit commits the specified ChangeSet, transforming the configuration on
	// which they are based into a new configuration, and storing that new
	// configuration as a next versions. Ensures that changes are applied as a
	// batch, are not applied more than once, and that new changes are not
	// started while a commit is underway
	Commit(version int, apply ApplyFn) error
}

// NewManager creates a new change list Manager
func NewManager(opts ManagerOptions) Manager {
	if opts == nil {
		panic("opts must not be nil")
	}

	if opts.GetConfigKey() == "" {
		panic("opts.ConfigKey must be set")
	}

	if opts.GetKV() == nil {
		panic("opts.KV must be set")
	}

	if opts.GetConfigType() == nil {
		panic("opts.ConfigType must be set")
	}

	if opts.GetChangesType() == nil {
		panic("opts.ChangesType must be set")
	}

	logger := opts.GetLogger()
	if logger == nil {
		logger = xlog.NullLogger
	}

	return &manager{
		key:         opts.GetConfigKey(),
		kv:          opts.GetKV(),
		configType:  proto.Clone(opts.GetConfigType()),
		changesType: proto.Clone(opts.GetChangesType()),
		log:         logger,
	}
}

type manager struct {
	key         string
	kv          kv.Store
	configType  proto.Message
	changesType proto.Message
	log         xlog.Logger
}

func (m manager) Change(change ChangeFn) error {
	for {
		// Retrieve the current configuration, creating an empty config if one does
		// not exist
		config := proto.Clone(m.configType)
		configVersion, err := m.getOrCreate(m.key, config)
		if err != nil {
			return err
		}

		// Retrieve the changes for the current configuration, creating an empty
		// change set if one does not exist
		changeset := &changesetpb.ChangeSet{
			ForVersion: int32(configVersion),
			State:      changesetpb.ChangeSetState_OPEN,
		}

		csKey := fmtChangeSetKey(m.key, configVersion)
		csVersion, err := m.getOrCreate(csKey, changeset)
		if err != nil {
			return err
		}

		// Only allow changes to an open change list
		if changeset.State != changesetpb.ChangeSetState_OPEN {
			return ErrCommitInProgress
		}

		// Apply the new changes...
		changes := proto.Clone(m.changesType)
		if err := proto.Unmarshal(changeset.Changes, changes); err != nil {
			return err
		}

		if err := change(config, changes); err != nil {
			return err
		}

		changeBytes, err := proto.Marshal(changes)
		if err != nil {
			return err
		}

		// ...and update the stored changes
		changeset.Changes = changeBytes
		if _, err := m.kv.CheckAndSet(csKey, csVersion, changeset); err != nil {
			if err == kv.ErrVersionMismatch {
				// Someone else updated the changes first - try again
				continue
			}

			return err
		}

		return nil
	}
}

func (m manager) Commit(version int, apply ApplyFn) error {
	for {
		// Get the current configuration, but don't bother trying to create it if it doesn't exist
		configVal, err := m.kv.Get(m.key)
		if err != nil {
			return err
		}

		// Confirm the version does exist...
		if configVal.Version() < version {
			return ErrUnknownVersion
		}

		// ...and that it hasn't already been committed
		if configVal.Version() > version {
			return ErrAlreadyCommitted
		}

		// Retrieve the config data so that we can transform it appropriately
		config := proto.Clone(m.configType)
		if err := configVal.Unmarshal(config); err != nil {
			return err
		}

		// Get the change list for the current configuration, again not bothering to create
		// if it doesn't exist
		csKey := fmtChangeSetKey(m.key, configVal.Version())
		csVal, err := m.kv.Get(csKey)
		if err != nil {
			return err
		}

		var changeset changesetpb.ChangeSet
		if err := csVal.Unmarshal(&changeset); err != nil {
			return err
		}

		// If the change list is already committed, bail out
		if changeset.State == changesetpb.ChangeSetState_COMMITTED {
			return ErrAlreadyCommitted
		}

		// Mark the change list as committing, so further changes are prevented

		// Transform the current configuration according to the change list
		changes := proto.Clone(m.changesType)
		if err := proto.Unmarshal(changeset.Changes, changes); err != nil {
			return err
		}
	}
}

func (m manager) getOrCreate(k string, v proto.Message) (int, error) {
	for {
		val, err := m.kv.Get(k)
		if err == kv.ErrNotFound {
			// Attempt to create
			version, err := m.kv.SetIfNotExists(k, v)
			if err == nil {
				return version, nil
			}

			if err == kv.ErrAlreadyExists {
				// Someone got there first...try again
				continue
			}

			return 0, err
		}

		if err != nil {
			// Some other error occurred
			return 0, err
		}

		// Unmarshall the current value
		if err := val.Unmarshal(v); err != nil {
			return 0, err
		}

		return val.Version(), nil
	}
}

func fmtChangeSetKey(configKey string, configVers int) string {
	return fmt.Sprintf("%s/_changes/%d", configKey, configVers)
}

type managerOptions struct {
	kv          kv.Store
	logger      xlog.Logger
	configKey   string
	configType  proto.Message
	changesType proto.Message
}

func (opts *managerOptions) GetKV() kv.Store               { return opts.kv }
func (opts *managerOptions) GetLogger() xlog.Logger        { return opts.logger }
func (opts *managerOptions) GetConfigKey() string          { return opts.configKey }
func (opts *managerOptions) GetConfigType() proto.Message  { return opts.configType }
func (opts *managerOptions) GetChangesType() proto.Message { return opts.changesType }

func (opts *managerOptions) KV(kv kv.Store) ManagerOptions {
	opts.kv = kv
	return opts
}
func (opts *managerOptions) Logger(logger xlog.Logger) ManagerOptions {
	opts.logger = logger
	return opts
}
func (opts *managerOptions) ConfigKey(k string) ManagerOptions {
	opts.configKey = k
	return opts
}
func (opts *managerOptions) ConfigType(ct proto.Message) ManagerOptions {
	opts.configType = ct
	return opts
}
func (opts *managerOptions) ChangesType(ct proto.Message) ManagerOptions {
	opts.changesType = ct
	return opts
}
