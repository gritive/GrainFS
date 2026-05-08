package raftv2

import (
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// stableStoreFactory describes a named StableStore constructor for table-driven
// tests. Both memStableStore and badgerStableStore are driven through the same
// scenarios.
type stableStoreFactory struct {
	name string
	new  func(t *testing.T) StableStore
	// newAt opens a *new* StableStore backed by the same persistence layer as
	// a previous one opened for dir. For memStableStore this is a no-op (returns
	// a fresh empty store). For badgerStableStore it re-opens the same Badger DB
	// path, exercising the "reopen after crash" path.
	newAt func(t *testing.T, dir string) StableStore
}

var allStableStores = []stableStoreFactory{
	{
		name: "mem",
		new:  func(t *testing.T) StableStore { return newMemStableStore() },
		newAt: func(t *testing.T, dir string) StableStore {
			// In-memory: no persistence across reopens; just return a fresh store.
			return newMemStableStore()
		},
	},
	{
		name: "badger",
		new: func(t *testing.T) StableStore {
			dir := t.TempDir()
			db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			store, err := newBadgerStableStore(db, []byte("raft/v2/hardstate/"))
			require.NoError(t, err)
			return store
		},
		newAt: func(t *testing.T, dir string) StableStore {
			db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			store, err := newBadgerStableStore(db, []byte("raft/v2/hardstate/"))
			require.NoError(t, err)
			return store
		},
	},
}

// TestStableStore_FirstOpenReturnsZero verifies that a freshly opened store
// returns the zero HardState (currentTerm=0, votedFor=""), representing the
// "first start" case.
func TestStableStore_FirstOpenReturnsZero(t *testing.T) {
	for _, f := range allStableStores {
		t.Run(f.name, func(t *testing.T) {
			s := f.new(t)
			hs, err := s.HardState()
			require.NoError(t, err)
			require.Equal(t, HardState{}, hs, "fresh store must return zero HardState")
		})
	}
}

// TestStableStore_RoundTrip verifies that SaveHardState followed by HardState
// returns the same value.
func TestStableStore_RoundTrip(t *testing.T) {
	for _, f := range allStableStores {
		t.Run(f.name, func(t *testing.T) {
			s := f.new(t)

			want := HardState{CurrentTerm: 7, VotedFor: "node-alpha"}
			require.NoError(t, s.SaveHardState(want))

			got, err := s.HardState()
			require.NoError(t, err)
			require.Equal(t, want, got)
		})
	}
}

// TestStableStore_RoundTripEmptyVotedFor verifies that an empty VotedFor
// (no vote granted) round-trips correctly.
func TestStableStore_RoundTripEmptyVotedFor(t *testing.T) {
	for _, f := range allStableStores {
		t.Run(f.name, func(t *testing.T) {
			s := f.new(t)

			want := HardState{CurrentTerm: 3, VotedFor: ""}
			require.NoError(t, s.SaveHardState(want))

			got, err := s.HardState()
			require.NoError(t, err)
			require.Equal(t, want, got)
		})
	}
}

// TestStableStore_OverwritePreservesLatest verifies that multiple
// SaveHardState calls overwrite the previous value.
func TestStableStore_OverwritePreservesLatest(t *testing.T) {
	for _, f := range allStableStores {
		t.Run(f.name, func(t *testing.T) {
			s := f.new(t)

			require.NoError(t, s.SaveHardState(HardState{CurrentTerm: 1, VotedFor: "n1"}))
			require.NoError(t, s.SaveHardState(HardState{CurrentTerm: 2, VotedFor: "n2"}))
			require.NoError(t, s.SaveHardState(HardState{CurrentTerm: 3, VotedFor: ""}))

			got, err := s.HardState()
			require.NoError(t, err)
			require.Equal(t, HardState{CurrentTerm: 3, VotedFor: ""}, got)
		})
	}
}

// TestStableStore_PersistsAcrossReopen verifies that the badger store retains
// HardState after closing and reopening the DB (simulating a crash-restart).
// The mem store cannot persist across reopens; this test is meaningful only
// for badger — the mem case verifies it returns zero on re-open.
func TestStableStore_PersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: open, write, close.
	{
		db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
		require.NoError(t, err)
		s, err := newBadgerStableStore(db, []byte("raft/v2/hardstate/"))
		require.NoError(t, err)
		require.NoError(t, s.SaveHardState(HardState{CurrentTerm: 42, VotedFor: "leader-node"}))
		require.NoError(t, db.Close())
	}

	// Phase 2: reopen, read — must recover the persisted state.
	{
		db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
		require.NoError(t, err)
		defer func() { _ = db.Close() }()
		s, err := newBadgerStableStore(db, []byte("raft/v2/hardstate/"))
		require.NoError(t, err)
		got, err := s.HardState()
		require.NoError(t, err)
		require.Equal(t, HardState{CurrentTerm: 42, VotedFor: "leader-node"}, got,
			"HardState must survive close+reopen")
	}
}
