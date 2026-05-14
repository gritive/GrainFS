package serveruntime

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/badgerrole"
	"github.com/gritive/GrainFS/internal/badgerutil"
	"github.com/gritive/GrainFS/internal/incident"
	"github.com/gritive/GrainFS/internal/incident/badgerstore"
)

// TestBootValidateConfig_AutogeneratesNodeID exercises the path where cfg has
// no nodeID. GenerateNodeID writes a node-id file in dataDir as a side effect.
func TestBootValidateConfig_AutogeneratesNodeID(t *testing.T) {
	dir := t.TempDir()
	state := newBootState(Config{DataDir: dir, ClusterKey: "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"})

	require.NoError(t, bootValidateConfig(state))
	assert.NotEmpty(t, state.nodeID, "auto-generated node ID must be set")
	assert.Equal(t, "127.0.0.1:0", state.raftAddr, "no peers, no raft-addr → loopback default")
	assert.True(t, state.clusterMode)
	assert.Equal(t, filepath.Join(dir, "meta"), state.metaDir)
	assert.Equal(t, filepath.Join(dir, "raft"), state.raftDir)
}

// TestBootAutoMigrate_NoOpOnFreshDir — empty dataDir means no legacy meta
// to migrate; bootAutoMigrate returns silently.
func TestBootAutoMigrate_NoOpOnFreshDir(t *testing.T) {
	state := newBootState(Config{DataDir: t.TempDir(), NodeID: "n1", ClusterKey: "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"})
	require.NoError(t, bootValidateConfig(state))

	require.NoError(t, bootAutoMigrate(state), "fresh dataDir is a no-op")
	_, err := os.Stat(state.raftDir)
	assert.True(t, os.IsNotExist(err), "auto-migrate must not create raftDir on a no-op")
}

// TestBootAutoMigrate_NoOpWhenRaftDirAlreadyExists — once raftDir exists the
// layout is already cluster-format; auto-migrate must not run again.
func TestBootAutoMigrate_NoOpWhenRaftDirAlreadyExists(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "raft"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "meta"), 0o755))
	// Drop a sentinel into meta to prove migrate did NOT touch it.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "meta", "marker"), []byte("untouched"), 0o644))

	state := newBootState(Config{DataDir: dir, NodeID: "n1", ClusterKey: "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"})
	require.NoError(t, bootValidateConfig(state))
	require.NoError(t, bootAutoMigrate(state))

	contents, err := os.ReadFile(filepath.Join(dir, "meta", "marker"))
	require.NoError(t, err)
	assert.Equal(t, "untouched", string(contents), "raftDir present → migrate is no-op")
}

// TestBootAutoMigrate_NoOpWhenMetaDirEmpty — raftDir absent + metaDir empty
// is a fresh deploy, not a legacy layout. No migration.
func TestBootAutoMigrate_NoOpWhenMetaDirEmpty(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "meta"), 0o755))

	state := newBootState(Config{DataDir: dir, NodeID: "n1", ClusterKey: "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"})
	require.NoError(t, bootValidateConfig(state))
	require.NoError(t, bootAutoMigrate(state))
}

// TestBootOpenMetaDB_CreatesAndOpens — happy path: fresh dataDir; phase
// creates metaDir, opens BadgerDB, registers cleanups, runs preflight.
// Verifies state fields are set and cleanup tears down without error.
func TestBootOpenMetaDB_CreatesAndOpens(t *testing.T) {
	state := newBootState(Config{DataDir: t.TempDir(), NodeID: "n1", ClusterKey: "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"})
	require.NoError(t, bootValidateConfig(state))

	require.NoError(t, bootOpenMetaDB(state))
	require.NotNil(t, state.db, "state.db populated after bootOpenMetaDB")
	require.NotEmpty(t, state.startupDecisions, "preflight decision recorded")
	assert.Equal(t, badgerrole.RoleMeta, state.startupDecisions[0].Role)
	assert.Equal(t, badgerrole.DecisionOK, state.startupDecisions[0].Status)

	// Cleanup must close the DB cleanly. Calling Cleanup twice is safe.
	state.Cleanup()
	state.Cleanup()
}

func TestBootOpenMetaDB_RecordsJournalOnOpenFailure(t *testing.T) {
	dir := t.TempDir()
	state := newBootState(Config{DataDir: dir, NodeID: "n1", ClusterKey: "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"})
	require.NoError(t, bootValidateConfig(state))
	require.NoError(t, os.WriteFile(state.metaDir, []byte("not-a-dir"), 0o644))

	err := bootOpenMetaDB(state)
	require.Error(t, err)

	pending, journalErr := badgerrole.PendingJournalEntries(dir)
	require.NoError(t, journalErr)
	require.Len(t, pending, 1)
	require.Equal(t, badgerrole.RoleMeta, pending[0].Decision.Role)
	require.Equal(t, badgerrole.DecisionOpenFailed, pending[0].Decision.Status)
	require.Equal(t, badgerrole.RecoveryActionBlockStart, pending[0].Decision.Action)
	require.Equal(t, "meta", pending[0].Decision.Path)
	require.NotEmpty(t, pending[0].BootID)
}

func TestRecordBadgerStartupDecisionResolvesMissingRolePath(t *testing.T) {
	dir := t.TempDir()
	state := newBootState(Config{DataDir: dir, NodeID: "n1", ClusterKey: "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"})
	require.NoError(t, bootValidateConfig(state))

	recordBadgerStartupDecision(state, badgerrole.Decision{
		Role:    badgerrole.RoleGroupState,
		GroupID: "group-a",
		Status:  badgerrole.DecisionOpenFailed,
		Action:  badgerrole.RecoveryActionStartReadOnly,
		Reason:  "open failed",
	})

	pending, err := badgerrole.PendingJournalEntries(dir)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, "groups/group-a/badger", pending[0].Decision.Path)
	require.Equal(t, filepath.Join(dir, "groups", "group-a", "badger"), state.startupDecisions[0].Path)
}

// TestBootValidateTimings_RejectsTooFastElection — election timeout below
// 3× heartbeat is rejected (Raft §5.2 timing requirement).
func TestBootValidateTimings_RejectsTooFastElection(t *testing.T) {
	state := newBootState(Config{
		RaftHeartbeatInterval: 100 * time.Millisecond,
		RaftElectionTimeout:   200 * time.Millisecond, // < 3× hb
	})
	err := bootValidateTimings(state)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be >= 3")
}

// TestBootValidateTimings_RejectsFlushNotMuchSmallerThanHeartbeat — quic mux
// flush window must be << raft heartbeat for hb to dispatch on time.
func TestBootValidateTimings_RejectsFlushNotMuchSmallerThanHeartbeat(t *testing.T) {
	state := newBootState(Config{
		QUICMuxEnabled:        true,
		QUICMuxFlushWindow:    100 * time.Millisecond,
		RaftHeartbeatInterval: 100 * time.Millisecond, // not <<
	})
	err := bootValidateTimings(state)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be <<")
}

// TestBootValidateTimings_AcceptsValidConfig — happy path: 200ms hb +
// 1s election + 2ms flush satisfies all three bounds.
func TestBootValidateTimings_AcceptsValidConfig(t *testing.T) {
	state := newBootState(Config{
		QUICMuxEnabled:        true,
		QUICMuxFlushWindow:    2 * time.Millisecond,
		RaftHeartbeatInterval: 200 * time.Millisecond,
		RaftElectionTimeout:   1 * time.Second,
	})
	assert.NoError(t, bootValidateTimings(state))
}

// TestBootOpenSharedFSMDB_Opens — happy path: a fresh dataDir gets a shared
// FSM-state BadgerDB at <dataDir>/shared-fsm/ and the matching startup decision.
func TestBootOpenSharedFSMDB_Opens(t *testing.T) {
	state := newBootState(Config{DataDir: t.TempDir(), NodeID: "n1", ClusterKey: "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"})
	require.NoError(t, bootValidateConfig(state))

	require.NoError(t, bootOpenSharedFSMDB(state))
	require.NotNil(t, state.sharedFSMDB)
	last := state.startupDecisions[len(state.startupDecisions)-1]
	assert.Equal(t, badgerrole.RoleSharedFSM, last.Role)

	state.Cleanup()
}

// TestBootOpenSharedFSMDB_IgnoresLegacyPerGroupDir — a stale pre-P3
// <dataDir>/groups/*/badger/ directory must NOT block boot. Pre-1.0, no
// migration: the phase opens the shared FSM DB and moves on.
func TestBootOpenSharedFSMDB_IgnoresLegacyPerGroupDir(t *testing.T) {
	dir := t.TempDir()
	legacyBadger := filepath.Join(dir, "groups", "group-0", "badger")
	require.NoError(t, os.MkdirAll(legacyBadger, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(legacyBadger, "MANIFEST"), []byte("legacy"), 0o644))

	state := newBootState(Config{DataDir: dir, NodeID: "n1", ClusterKey: "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"})
	require.NoError(t, bootValidateConfig(state))

	require.NoError(t, bootOpenSharedFSMDB(state))
	require.NotNil(t, state.sharedFSMDB)
	last := state.startupDecisions[len(state.startupDecisions)-1]
	assert.Equal(t, badgerrole.RoleSharedFSM, last.Role)

	state.Cleanup()
}

func TestImportBadgerRecoveryJournalRecordsIncidentAndMarksImported(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	writer := badgerrole.NewJournalWriter(badgerrole.JournalOptions{
		DataDir: dataDir,
		NodeID:  "n1",
		Now:     func() time.Time { return time.Date(2026, 5, 15, 1, 2, 3, 0, time.UTC) },
	})
	entry, err := writer.Record(badgerrole.Decision{
		Role:   badgerrole.RoleSharedFSM,
		Path:   filepath.Join(dataDir, "shared-fsm"),
		Status: badgerrole.DecisionOpenFailed,
		Action: badgerrole.RecoveryActionBlockStart,
		Reason: "open shared FSM-state badger: disk full",
	}, badgerrole.StartupModeBlocked)
	require.NoError(t, err)

	db, err := badger.Open(badgerutil.SmallOptions(t.TempDir()))
	require.NoError(t, err)
	defer db.Close()
	store := badgerstore.New(db)

	imported, err := importBadgerRecoveryJournal(ctx, store, dataDir)
	require.NoError(t, err)
	require.Equal(t, 1, imported)

	state, ok, err := store.Get(ctx, entry.IncidentID)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, incident.CauseBadgerOpenFailed, state.Cause)
	require.Equal(t, incident.ActionBlockStartup, state.Action)
	require.Equal(t, incident.ScopeBadgerRole, state.Scope.Kind)
	require.Equal(t, string(badgerrole.RoleSharedFSM), state.Scope.BadgerRole)
	require.Equal(t, "shared-fsm", state.Scope.Path)
	require.Contains(t, state.Decision, "disk full")

	pending, err := badgerrole.PendingJournalEntries(dataDir)
	require.NoError(t, err)
	require.Empty(t, pending)

	imported, err = importBadgerRecoveryJournal(ctx, store, dataDir)
	require.NoError(t, err)
	require.Equal(t, 0, imported)
}

func TestImportBadgerRecoveryJournalDoesNotRegressExistingIncident(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	observed := time.Date(2026, 5, 15, 1, 2, 3, 0, time.UTC)
	writer := badgerrole.NewJournalWriter(badgerrole.JournalOptions{
		DataDir: dataDir,
		NodeID:  "n1",
		Now:     func() time.Time { return observed },
	})
	entry, err := writer.Record(badgerrole.Decision{
		Role:   badgerrole.RoleMeta,
		Path:   filepath.Join(dataDir, "meta"),
		Status: badgerrole.DecisionOpenFailed,
		Action: badgerrole.RecoveryActionBlockStart,
		Reason: "open failed",
	}, badgerrole.StartupModeBlocked)
	require.NoError(t, err)

	db, err := badger.Open(badgerutil.SmallOptions(t.TempDir()))
	require.NoError(t, err)
	defer db.Close()
	store := badgerstore.New(db)
	existing := incident.IncidentState{
		ID:          entry.IncidentID,
		State:       incident.StateFixed,
		Severity:    incident.SeverityInfo,
		Cause:       incident.CauseBadgerOpenFailed,
		Action:      incident.ActionBlockStartup,
		Scope:       incident.Scope{Kind: incident.ScopeBadgerRole, BadgerRole: string(badgerrole.RoleMeta), Path: "meta"},
		Proof:       incident.Proof{Status: incident.ProofNotRequired},
		NextAction:  "No action needed.",
		ObservedAt:  observed.Add(-time.Hour),
		UpdatedAt:   observed.Add(-time.Minute),
		CompletedAt: observed.Add(-time.Minute),
	}
	require.NoError(t, store.Put(ctx, existing))

	imported, err := importBadgerRecoveryJournal(ctx, store, dataDir)
	require.NoError(t, err)
	require.Equal(t, 0, imported)

	got, ok, err := store.Get(ctx, entry.IncidentID)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, incident.StateFixed, got.State)
	require.Equal(t, existing.UpdatedAt, got.UpdatedAt)

	pending, err := badgerrole.PendingJournalEntries(dataDir)
	require.NoError(t, err)
	require.Empty(t, pending)
}

// TestBootMetaDB_PreflightRejectsCorruptDB — when the BadgerDB on disk fails
// the writability probe (e.g., a corrupt manifest), bootOpenMetaDB returns
// a structured PreflightBadger error instead of plain badger.Open output.
//
// We simulate "corrupt" by pre-opening a DB and writing a sentinel through
// it, then re-opening with a sentinel that fails — the cheaper alternative
// is to touch the underlying check directly. For now the DecisionOK path is
// exercised by TestBootOpenMetaDB_CreatesAndOpens; the preflight-failure
// path is covered by server.PreflightBadger's own tests in internal/server.
//
// This stub exists to document the intent and surface a TODO if ProbeWritable
// gains a forced-failure injection point.
func TestBootMetaDB_PreflightRejectsCorruptDB(t *testing.T) {
	t.Skip("preflight rejection requires DB corruption that cannot be safely simulated in unit; covered by integration tests")
}

func TestBootValidateConfig_JoinPendingFile(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, ".join-pending"), []byte("127.0.0.1:9999"), 0o600))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "meta_raft"), 0o755))

	state := newBootState(Config{
		DataDir:    dir,
		NodeID:     "n1",
		RaftAddr:   "127.0.0.1:8301",
		ClusterKey: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	})
	require.NoError(t, bootValidateConfig(state))

	assert.True(t, state.joinMode)
	assert.Equal(t, "127.0.0.1:9999", state.joinAddr)
	_, err := os.Stat(filepath.Join(dir, "meta_raft"))
	assert.True(t, os.IsNotExist(err))
	_, err = os.Stat(filepath.Join(dir, "meta_raft.pre-join-backup"))
	assert.NoError(t, err)
}

func TestBootValidateConfig_JoinPendingFile_EmptyPeer(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, ".join-pending"), []byte("   "), 0o600))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "meta_raft"), 0o755))

	state := newBootState(Config{DataDir: dir, NodeID: "n1", ClusterKey: "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"})
	require.NoError(t, bootValidateConfig(state))

	assert.False(t, state.joinMode)
	_, err := os.Stat(filepath.Join(dir, "meta_raft"))
	assert.NoError(t, err)
}

func TestBootValidateConfig_NoJoinPending_SoloBootstrap(t *testing.T) {
	dir := t.TempDir()
	state := newBootState(Config{DataDir: dir, NodeID: "n1", ClusterKey: "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"})
	require.NoError(t, bootValidateConfig(state))
	assert.False(t, state.joinMode)
}

func TestBootValidateConfig_ExistingRaftState_Reconnect(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "meta_raft"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "meta_raft", "MANIFEST"), []byte("data"), 0o600))

	state := newBootState(Config{DataDir: dir, NodeID: "n1", ClusterKey: "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"})
	require.NoError(t, bootValidateConfig(state))
	assert.False(t, state.joinMode)
	_, err := os.Stat(filepath.Join(dir, "meta_raft"))
	assert.NoError(t, err)
}
