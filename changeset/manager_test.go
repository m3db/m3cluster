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
	"reflect"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/proto"
	"github.com/m3db/m3cluster/generated/proto/changesetpb"
	"github.com/m3db/m3cluster/generated/proto/changesettest"
	"github.com/m3db/m3cluster/kv"
	"github.com/stretchr/testify/require"
)

var (
	errBadThingsHappened = errors.New("bad things happened")
)

func TestManager_ChangeEmptyInitialConfig(t *testing.T) {
	s := newTestSuite(t)
	defer s.finish()

	var (
		config1  = new(configMatcher)
		changes1 = new(changeSetMatcher)
		changes2 = new(changeSetMatcher)
	)

	gomock.InOrder(
		// Get initial config - see no value and create
		s.kv.EXPECT().Get("config").Return(nil, kv.ErrNotFound),
		s.kv.EXPECT().SetIfNotExists("config", config1).Return(1, nil),

		// Get initial changes - see no value and create
		s.kv.EXPECT().Get("config/_changes/1").Return(nil, kv.ErrNotFound),
		s.kv.EXPECT().SetIfNotExists("config/_changes/1", changes1).Return(1, nil),
		s.kv.EXPECT().CheckAndSet("config/_changes/1", 1, changes2).Return(2, nil),
	)

	require.NoError(t, s.mgr.Change(addLines("foo", "bar")))

	require.Equal(t, "", config1.config().Text)

	require.Equal(t, int32(1), changes1.changeset(t).ForVersion)
	require.Equal(t, changesetpb.ChangeSetState_OPEN, changes1.changeset(t).State)
	require.Nil(t, changes1.changeset(t).Changes)

	require.Equal(t, int32(1), changes2.changeset(t).ForVersion)
	require.Equal(t, changesetpb.ChangeSetState_OPEN, changes2.changeset(t).State)
	require.NotNil(t, changes2.changeset(t).Changes)
	require.Equal(t, []string{"foo", "bar"}, changes2.changes(t).Lines)
}

func TestManager_ChangeInterruptOnCreateOfInitialConfig(t *testing.T) {
	s := newTestSuite(t)
	defer s.finish()

	var (
		config1    = new(configMatcher)
		config2    = new(configMatcher)
		changes1   = new(changeSetMatcher)
		changes2   = new(changeSetMatcher)
		configVal  = s.newMockValue()
		changesVal = s.newMockValue()
	)

	gomock.InOrder(
		// Initial attempt to create config - someone else gets there first
		s.kv.EXPECT().Get("config").Return(nil, kv.ErrNotFound),
		s.kv.EXPECT().SetIfNotExists("config", config1).Return(0, kv.ErrAlreadyExists),

		// Will refetch
		s.kv.EXPECT().Get("config").Return(configVal, nil),
		configVal.EXPECT().Unmarshal(config2).Return(nil),

		// Fetch corresponding changes
		configVal.EXPECT().Version().Return(2),
		s.kv.EXPECT().Get("config/_changes/2").Return(changesVal, nil),
		changesVal.EXPECT().Unmarshal(changes1).Return(nil),
		changesVal.EXPECT().Version().Return(12),

		// ...And update
		s.kv.EXPECT().CheckAndSet("config/_changes/2", 12, changes2).Return(13, nil),
	)

	require.NoError(t, s.mgr.Change(addLines("foo", "bar")))

	// NB(mmihic): We only care that the expectations are met
}

func TestManager_ChangeInterruptOnCreateOfInitialChangeSet(t *testing.T) {
	s := newTestSuite(t)
	defer s.finish()

	var (
		changes1   = new(changeSetMatcher)
		changes2   = new(changeSetMatcher)
		changes3   = new(changeSetMatcher)
		changesVal = s.newMockValue()
	)

	gomock.InOrder(
		s.mockGetOrCreate("config", &changesettest.Config{}, 13),

		// Initial attempt to create changes - someone else gets there first
		s.kv.EXPECT().Get("config/_changes/13").Return(nil, kv.ErrNotFound),
		s.kv.EXPECT().SetIfNotExists("config/_changes/13", changes1).Return(0, kv.ErrAlreadyExists),

		// Will refetch
		s.kv.EXPECT().Get("config/_changes/13").Return(changesVal, nil),
		changesVal.EXPECT().Unmarshal(changes2).Return(nil),
		changesVal.EXPECT().Version().Return(12),

		// ...And update
		s.kv.EXPECT().CheckAndSet("config/_changes/13", 12, changes3).Return(13, nil),
	)

	require.NoError(t, s.mgr.Change(addLines("foo", "bar")))

	// NB(mmihic): We only care that the expectations are met
}

func TestManager_ChangeErrorRetrievingConfig(t *testing.T) {
	s := newTestSuite(t)
	defer s.finish()

	// Initial attempt to get changes fails
	s.kv.EXPECT().Get("config").Return(nil, errBadThingsHappened)

	err := s.mgr.Change(addLines("foo", "bar"))
	require.Equal(t, errBadThingsHappened, err)

	// NB(mmihic): We only care that the expectations are met
}

func TestManager_ChangeErrorRetrievingChangeSet(t *testing.T) {
	s := newTestSuite(t)
	defer s.finish()

	gomock.InOrder(
		s.mockGetOrCreate("config", &changesettest.Config{}, 13),
		s.kv.EXPECT().Get("config/_changes/13").Return(nil, errBadThingsHappened),
	)

	err := s.mgr.Change(addLines("foo", "bar"))
	require.Equal(t, errBadThingsHappened, err)

	// NB(mmihic): We only care that the expectations are met
}

func TestManager_ChangeErrorUnmarshallingInitialChange(t *testing.T) {
	s := newTestSuite(t)
	defer s.finish()

	gomock.InOrder(
		s.mockGetOrCreate("config", &changesettest.Config{}, 13),
		s.mockGetOrCreate("config/_changes/13", &changesetpb.ChangeSet{
			ForVersion: 13,
			State:      changesetpb.ChangeSetState_OPEN,
			Changes:    []byte("foo"), // Not a valid proto
		}, 12),
	)

	require.Error(t, s.mgr.Change(addLines("foo", "bar")))
}

func TestManager_ChangeErrorUpdatingChangeSet(t *testing.T) {
	s := newTestSuite(t)
	defer s.finish()

	var (
		updatedChanges = new(changeSetMatcher)
	)

	gomock.InOrder(
		s.mockGetOrCreate("config", &changesettest.Config{}, 13),
		s.mockGetOrCreate("config/_changes/13",
			s.newOpenChangeSet(13, &changesettest.Changes{}), 12),
		s.kv.EXPECT().CheckAndSet("config/_changes/13", 12, updatedChanges).
			Return(0, errBadThingsHappened),
	)

	err := s.mgr.Change(addLines("foo", "bar"))
	require.Equal(t, errBadThingsHappened, err)
}

func TestManager_ChangeVersionMismatchUpdatingChangeSet(t *testing.T) {
	s := newTestSuite(t)
	defer s.finish()

	var (
		changes1 = new(changeSetMatcher)
		changes2 = new(changeSetMatcher)
	)

	gomock.InOrder(
		// Version mismatch while updating changeset
		s.mockGetOrCreate("config", &changesettest.Config{}, 13),
		s.mockGetOrCreate("config/_changes/13", s.newOpenChangeSet(13, &changesettest.Changes{}), 12),
		s.kv.EXPECT().CheckAndSet("config/_changes/13", 12, changes1).
			Return(0, kv.ErrVersionMismatch),

		// Will try again
		s.mockGetOrCreate("config", &changesettest.Config{}, 14),
		s.mockGetOrCreate("config/_changes/14", s.newOpenChangeSet(14, &changesettest.Changes{
			Lines: []string{"zed"},
		}), 22),
		s.kv.EXPECT().CheckAndSet("config/_changes/14", 22, changes2).Return(23, nil),
	)

	require.NoError(t, s.mgr.Change(addLines("foo", "bar")))
	require.Equal(t, []string{"zed", "foo", "bar"}, changes2.changes(t).Lines)
}

func TestManager_ChangeSuccess(t *testing.T) {
	s := newTestSuite(t)
	defer s.finish()

	updatedChanges := new(changeSetMatcher)
	gomock.InOrder(
		s.mockGetOrCreate("config", &changesettest.Config{}, 72),
		s.mockGetOrCreate("config/_changes/72", s.newOpenChangeSet(72, &changesettest.Changes{
			Lines: []string{"ark", "bork"},
		}), 29),
		s.kv.EXPECT().CheckAndSet("config/_changes/72", 29, updatedChanges).Return(23, nil),
	)

	require.NoError(t, s.mgr.Change(addLines("foo", "bar")))
	require.Equal(t, updatedChanges.changes(t).Lines, []string{"ark", "bork", "foo", "bar"})
}

func TestManager_ChangeOnCommittedChangeSet(t *testing.T) {
	s := newTestSuite(t)
	defer s.finish()

	gomock.InOrder(
		s.mockGetOrCreate("config", &changesettest.Config{}, 72),
		s.mockGetOrCreate("config/_changes/72", s.newChangeSet(72, changesetpb.ChangeSetState_COMMITTED,
			&changesettest.Changes{
				Lines: []string{"ark", "bork"},
			}), 29),
	)

	err := s.mgr.Change(addLines("foo", "bar"))
	require.Equal(t, ErrCommitInProgress, err)
}

func TestManager_ChangeOnCommittingChangeSet(t *testing.T) {
	s := newTestSuite(t)
	defer s.finish()

	gomock.InOrder(
		s.mockGetOrCreate("config", &changesettest.Config{}, 72),
		s.mockGetOrCreate("config/_changes/72", s.newChangeSet(72, changesetpb.ChangeSetState_COMMITTING,
			&changesettest.Changes{
				Lines: []string{"ark", "bork"},
			}), 29),
	)

	err := s.mgr.Change(addLines("foo", "bar"))
	require.Equal(t, ErrCommitInProgress, err)
}

func TestManager_ChangeFunctionFails(t *testing.T) {
	s := newTestSuite(t)
	defer s.finish()

	gomock.InOrder(
		s.mockGetOrCreate("config", &changesettest.Config{}, 72),
		s.mockGetOrCreate("config/_changes/72", s.newOpenChangeSet(72, &changesettest.Changes{
			Lines: []string{"ark", "bork"},
		}), 29),
	)

	err := s.mgr.Change(func(cfg, changes proto.Message) error {
		return errBadThingsHappened
	})
	require.Equal(t, errBadThingsHappened, err)
}

func TestManagerCommit_Success(t *testing.T) {
	s := newTestSuite(t)
	defer s.finish()

	var (
		changeSet1 = new(changeSetMatcher)
		changeSet2 = new(changeSetMatcher)
		config1    = new(configMatcher)

		committedVersion = 22
		changeSetVersion = 17
		changeSetKey     = fmtChangeSetKey(s.configKey, committedVersion)
	)

	gomock.InOrder(
		// Retrieve the config value
		s.kv.EXPECT().Get(s.configKey).Return(kv.NewFakeValue(committedVersion,
			&changesettest.Config{
				Text: "shoop\nwoop\nhoop",
			}), nil),

		// Retrieve the change set
		s.kv.EXPECT().Get(changeSetKey).Return(kv.NewFakeValue(changeSetVersion,
			s.newOpenChangeSet(changeSetVersion, &changesettest.Changes{
				Lines: []string{"foo", "bar"},
			})), nil),

		// Mark as committing
		s.kv.EXPECT().CheckAndSet(changeSetKey, changeSetVersion, changeSet1).
			Return(changeSetVersion+1, nil),

		// Update the transformed confi
		s.kv.EXPECT().CheckAndSet(s.configKey, committedVersion, config1).
			Return(committedVersion+1, nil),

		// Mark as committed
		s.kv.EXPECT().CheckAndSet(changeSetKey, changeSetVersion+1, changeSet2).
			Return(changeSetVersion+2, nil),
	)

	err := s.mgr.Commit(committedVersion, commit)
	require.NoError(t, err)

	require.Equal(t, changesetpb.ChangeSetState_COMMITTING, changeSet1.changeset(t).State)
	require.Equal(t, changesetpb.ChangeSetState_COMMITTED, changeSet2.changeset(t).State)
	require.Equal(t, "shoop\nwoop\nhoop\nfoo\nbar", config1.config().Text)
}

func TestManagerCommit_ConfigNotFound(t *testing.T) {
}

func TestManagerCommit_ConfigGetError(t *testing.T) {
}

func TestManagerCommit_ConfigAtEarlierVersion(t *testing.T) {
}

func TestManagerCommit_ConfigAtLaterVersion(t *testing.T) {
}

func TestManagerCommit_ConfigUnmarshalError(t *testing.T) {
}

func TestManagerCommit_ChangeSetGetError(t *testing.T) {
}

func TestManagerCommit_ChangeSetUnmarshalError(t *testing.T) {
}

func TestManagerCommit_ChangeSetCommitted(t *testing.T) {
}

func TestMangerCommit_ChangeSetErrorMarkingAsCommitting(t *testing.T) {
}

func TestManagerCommit_ChangeSetVersionMismatchMarkingAsCommitting(t *testing.T) {
}

func TestManagerCommit_ChangesUnmarshalError(t *testing.T) {
}

func TestManagerCommit_CommitFunctionError(t *testing.T) {
}

func TestManagerCommit_ConfigUpdateVersionMismatch(t *testing.T) {
}

func TestManagerCommit_ConfigUpdateError(t *testing.T) {
}

func TestManagerCommit_ChangeSetErrorMarkingAsCommitted(t *testing.T) {
}

func TestManagerCommit_ChangeSetVersionMismatchMarkingAsCommitted(t *testing.T) {
}

type configMatcher struct {
	CapturingProtoMatcher
}

func (m *configMatcher) config() *changesettest.Config {
	return m.Arg.(*changesettest.Config)
}

type changeSetMatcher struct {
	CapturingProtoMatcher
}

func (m *changeSetMatcher) changeset(t *testing.T) *changesetpb.ChangeSet {
	return m.Arg.(*changesetpb.ChangeSet)
}

func (m *changeSetMatcher) changes(t *testing.T) *changesettest.Changes {
	changes := new(changesettest.Changes)
	require.NoError(t, proto.Unmarshal(m.changeset(t).Changes, changes))
	return changes
}

func addLines(lines ...string) ChangeFn {
	return func(cfgProto, changesProto proto.Message) error {
		changes := changesProto.(*changesettest.Changes)
		changes.Lines = append(changes.Lines, lines...)
		return nil
	}
}

func commit(cfgProto, changesProto proto.Message) error {
	changes := changesProto.(*changesettest.Changes)
	config := cfgProto.(*changesettest.Config)
	if config.Text != "" {
		config.Text = config.Text + "\n"
	}
	config.Text = config.Text + strings.Join(changes.Lines, "\n")
	return nil
}

type testSuite struct {
	t         *testing.T
	kv        *kv.MockStore
	mc        *gomock.Controller
	mgr       Manager
	configKey string
}

func newTestSuite(t *testing.T) *testSuite {
	mc := gomock.NewController(t)
	kvStore := kv.NewMockStore(mc)
	configKey := "config"
	mgr := NewManager(NewManagerOptions().
		KV(kvStore).
		ConfigType(&changesettest.Config{}).
		ChangesType(&changesettest.Changes{}).
		ConfigKey(configKey))

	return &testSuite{
		t:         t,
		mc:        mc,
		kv:        kvStore,
		configKey: configKey,
		mgr:       mgr,
	}
}

func (t *testSuite) finish() {
	t.mc.Finish()
}

func (t *testSuite) newMockValue() *kv.MockValue {
	return kv.NewMockValue(t.mc)
}

func (t *testSuite) mockUnmarshal(val *kv.MockValue, msg proto.Message) *gomock.Call {
	// SetArg requires a non-pointer type
	unPtr := reflect.ValueOf(msg).Elem().Interface()
	return val.EXPECT().Unmarshal(gomock.Any()).SetArg(0, unPtr).Return(nil)
}

func (t *testSuite) mockGetOrCreate(key string, msg proto.Message, vers int) *gomock.Call {
	val := t.newMockValue()
	c1 := t.kv.EXPECT().Get(key).Return(val, nil)
	c2 := t.mockUnmarshal(val, msg).After(c1)
	c3 := val.EXPECT().Version().Return(vers).After(c2)
	return c3
}

func (t *testSuite) newOpenChangeSet(forVersion int, changes proto.Message) *changesetpb.ChangeSet {
	return t.newChangeSet(forVersion, changesetpb.ChangeSetState_OPEN, changes)
}

func (t *testSuite) newChangeSet(forVersion int, state changesetpb.ChangeSetState, changes proto.Message,
) *changesetpb.ChangeSet {
	changeSet := &changesetpb.ChangeSet{
		ForVersion: int32(forVersion),
		State:      state,
	}

	cbytes, err := proto.Marshal(changes)
	require.NoError(t.t, err)
	changeSet.Changes = cbytes
	return changeSet
}

type CapturingProtoMatcher struct {
	Arg proto.Message
}

func (m *CapturingProtoMatcher) Matches(arg interface{}) bool {
	msg, ok := arg.(proto.Message)
	if !ok {
		return false
	}

	m.Arg = proto.Clone(msg)
	return true
}

func (m *CapturingProtoMatcher) String() string {
	return "proto-capture"
}
