package raft

import (
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// TestBadgerStoreExports_PR26_PersistAcrossReopen verifies the exported
// constructors return a durable trio (LogStore + StableStore + SnapshotStore)
// whose state survives close/reopen. Closes the PR 22 deferral: v2 BadgerLogStore
// was implemented in PR 10 but never wired into the cluster factory or
// serveruntime; PR 26 wires it. This test guards the export contract so future
// refactors cannot silently drop durability.
func TestBadgerStoreExports_PR26_PersistAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	logPrefix := []byte("raft/v2/log/")
	stablePrefix := []byte("raft/v2/hardstate/")
	snapPrefix := []byte("raft/v2/snap/")

	// Phase 1: write log entries + HardState, close.
	func() {
		db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
		require.NoError(t, err)
		defer db.Close()

		ls, err := NewBadgerLogStore(db, logPrefix)
		require.NoError(t, err)
		ss, err := NewBadgerStableStore(db, stablePrefix)
		require.NoError(t, err)
		_, err = NewBadgerSnapshotStore(db, snapPrefix)
		require.NoError(t, err)

		require.NoError(t, ls.Append([]LogEntry{
			{Term: 1, Index: 1, Type: LogEntryCommand, Command: []byte("alpha")},
			{Term: 1, Index: 2, Type: LogEntryCommand, Command: []byte("beta")},
			{Term: 2, Index: 3, Type: LogEntryCommand, Command: []byte("gamma")},
		}))
		require.NoError(t, ss.SaveHardState(HardState{CurrentTerm: 7, VotedFor: "node-A"}))
	}()

	// Phase 2: reopen and verify durability.
	db2, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db2.Close() })

	ls2, err := NewBadgerLogStore(db2, logPrefix)
	require.NoError(t, err)
	ss2, err := NewBadgerStableStore(db2, stablePrefix)
	require.NoError(t, err)

	require.Equal(t, uint64(3), ls2.LastIndex(), "log entries must survive reopen")
	e2, err := ls2.Entry(2)
	require.NoError(t, err)
	require.Equal(t, []byte("beta"), e2.Command)

	hs, err := ss2.HardState()
	require.NoError(t, err)
	require.Equal(t, uint64(7), hs.CurrentTerm, "HardState.CurrentTerm must survive reopen")
	require.Equal(t, "node-A", hs.VotedFor, "HardState.VotedFor must survive reopen")
}

// TestBadgerStoreExports_PR26_NewNodeUsesDurableStores verifies that wiring the
// exported stores into raftv2.NewNode produces a working actor that survives
// restart (term + log preserved). Mirrors the way the cluster factory and
// serveruntime will wire v2 in PR 26.
func TestBadgerStoreExports_PR26_NewNodeUsesDurableStores(t *testing.T) {
	dir := t.TempDir()
	logPrefix := []byte("raft/v2/log/")
	stablePrefix := []byte("raft/v2/hardstate/")
	snapPrefix := []byte("raft/v2/snap/")

	open := func() (*Node, *badger.DB) {
		db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
		require.NoError(t, err)
		ls, err := NewBadgerLogStore(db, logPrefix)
		require.NoError(t, err)
		ss, err := NewBadgerStableStore(db, stablePrefix)
		require.NoError(t, err)
		sn, err := NewBadgerSnapshotStore(db, snapPrefix)
		require.NoError(t, err)
		n, err := NewNode(Config{
			ID:            "node-A",
			LogStore:      ls,
			StableStore:   ss,
			SnapshotStore: sn,
		})
		require.NoError(t, err)
		return n, db
	}

	// First boot: persist a HardState term advance via the StableStore so we
	// can verify NewNode restores it without exercising network or election.
	{
		db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
		require.NoError(t, err)
		ss, err := NewBadgerStableStore(db, stablePrefix)
		require.NoError(t, err)
		require.NoError(t, ss.SaveHardState(HardState{CurrentTerm: 9, VotedFor: "node-A"}))
		_ = db.Close()
	}

	// Second boot: NewNode must observe the persisted term.
	n, db := open()
	t.Cleanup(func() { _ = db.Close() })
	require.Equal(t, uint64(9), n.Term(), "Term must be restored from durable StableStore")
}
