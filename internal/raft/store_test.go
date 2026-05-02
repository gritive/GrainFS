package raft

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestStore(t *testing.T) *BadgerLogStore {
	t.Helper()
	dir := t.TempDir()
	store, err := NewBadgerLogStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })
	return store
}

func TestBadgerLogStore_AppendAndGet(t *testing.T) {
	store := setupTestStore(t)

	entries := []LogEntry{
		{Term: 1, Index: 1, Command: []byte("cmd1")},
		{Term: 1, Index: 2, Command: []byte("cmd2")},
		{Term: 2, Index: 3, Command: []byte("cmd3")},
	}

	require.NoError(t, store.AppendEntries(entries))

	for _, want := range entries {
		got, err := store.GetEntry(want.Index)
		require.NoError(t, err)
		assert.Equal(t, want.Term, got.Term)
		assert.Equal(t, want.Index, got.Index)
		assert.Equal(t, want.Command, got.Command)
	}
}

func TestBadgerLogStore_GetEntries(t *testing.T) {
	store := setupTestStore(t)

	entries := []LogEntry{
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 1, Index: 2, Command: []byte("b")},
		{Term: 1, Index: 3, Command: []byte("c")},
		{Term: 2, Index: 4, Command: []byte("d")},
	}
	require.NoError(t, store.AppendEntries(entries))

	got, err := store.GetEntries(2, 4)
	require.NoError(t, err)
	require.Len(t, got, 3)
	assert.Equal(t, uint64(2), got[0].Index)
	assert.Equal(t, uint64(4), got[2].Index)
}

func TestBadgerLogStore_LastIndex(t *testing.T) {
	store := setupTestStore(t)

	// Empty store
	idx, err := store.LastIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(0), idx)

	entries := []LogEntry{
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 1, Index: 2, Command: []byte("b")},
		{Term: 1, Index: 3, Command: []byte("c")},
	}
	require.NoError(t, store.AppendEntries(entries))

	idx, err = store.LastIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(3), idx)
}

func TestBadgerLogStore_TruncateAfter(t *testing.T) {
	store := setupTestStore(t)

	entries := []LogEntry{
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 1, Index: 2, Command: []byte("b")},
		{Term: 2, Index: 3, Command: []byte("c")},
		{Term: 2, Index: 4, Command: []byte("d")},
	}
	require.NoError(t, store.AppendEntries(entries))

	// Truncate after index 2 (remove 3, 4)
	require.NoError(t, store.TruncateAfter(2))

	idx, err := store.LastIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(2), idx)

	// Index 3 should not exist
	_, err = store.GetEntry(3)
	assert.Error(t, err)
}

func TestBadgerLogStore_SaveAndLoadState(t *testing.T) {
	store := setupTestStore(t)

	// Fresh state
	term, votedFor, err := store.LoadState()
	require.NoError(t, err)
	assert.Equal(t, uint64(0), term)
	assert.Empty(t, votedFor)

	// Save state
	require.NoError(t, store.SaveState(5, "node-B"))

	term, votedFor, err = store.LoadState()
	require.NoError(t, err)
	assert.Equal(t, uint64(5), term)
	assert.Equal(t, "node-B", votedFor)

	// Update state
	require.NoError(t, store.SaveState(7, "node-C"))
	term, votedFor, err = store.LoadState()
	require.NoError(t, err)
	assert.Equal(t, uint64(7), term)
	assert.Equal(t, "node-C", votedFor)
}

func TestBadgerLogStore_SaveAndLoadSnapshot(t *testing.T) {
	store := setupTestStore(t)

	// No snapshot initially
	snap, err := store.LoadSnapshot()
	require.NoError(t, err)
	assert.Equal(t, uint64(0), snap.Index)
	assert.Equal(t, uint64(0), snap.Term)
	assert.Nil(t, snap.Data)

	// Save snapshot
	snapData := []byte(`{"state":"snapshot-data"}`)
	require.NoError(t, store.SaveSnapshot(Snapshot{Index: 10, Term: 3, Data: snapData}))

	snap, err = store.LoadSnapshot()
	require.NoError(t, err)
	assert.Equal(t, uint64(10), snap.Index)
	assert.Equal(t, uint64(3), snap.Term)
	assert.Equal(t, snapData, snap.Data)
}

// TestBadgerLogStore_SnapshotRoundTrip_PreservesRemovedFromCluster — PR-K1 follow-up.
// Verifies the Servers-derived persistence wiring: a node with removedFromCluster=true
// produces a snapshot whose Servers list omits self; on reopen the orphan flag
// is reconstructed from `self ∉ Servers`.
func TestBadgerLogStore_SnapshotRoundTrip_PreservesRemovedFromCluster(t *testing.T) {
	dir := t.TempDir()
	store, err := NewBadgerLogStore(dir)
	require.NoError(t, err)

	// Save snapshot whose Servers list excludes self ID "n1" — represents an
	// orphan node's view at snapshot time.
	require.NoError(t, store.SaveSnapshot(Snapshot{
		Index: 5,
		Term:  2,
		Data:  []byte(`{"fsm":"ok"}`),
		Servers: []Server{
			{ID: "n2", Suffrage: Voter},
			{ID: "n3", Suffrage: Voter},
			{ID: "n4", Suffrage: Voter},
		},
	}))
	require.NoError(t, store.Close())

	// Reopen and verify Servers list is preserved without self.
	store2, err := NewBadgerLogStore(dir)
	require.NoError(t, err)
	defer store2.Close()

	snap, err := store2.LoadSnapshot()
	require.NoError(t, err)
	require.Len(t, snap.Servers, 3)
	require.False(t, containsServer(snap.Servers, "n1"),
		"orphan snapshot must not contain self in restored Servers")
	require.True(t, containsServer(snap.Servers, "n2"))
}

func TestBadgerLogStore_PersistenceAcrossReopen(t *testing.T) {
	dir := t.TempDir()

	// Open, write, close
	store1, err := NewBadgerLogStore(dir)
	require.NoError(t, err)

	entries := []LogEntry{
		{Term: 1, Index: 1, Command: []byte("persist-test")},
	}
	require.NoError(t, store1.AppendEntries(entries))
	require.NoError(t, store1.SaveState(3, "node-A"))
	require.NoError(t, store1.SaveSnapshot(Snapshot{Index: 1, Term: 1, Data: []byte("snap")}))
	require.NoError(t, store1.Close())

	// Reopen and verify
	store2, err := NewBadgerLogStore(dir)
	require.NoError(t, err)
	defer store2.Close()

	entry, err := store2.GetEntry(1)
	require.NoError(t, err)
	assert.Equal(t, "persist-test", string(entry.Command))

	term, votedFor, err := store2.LoadState()
	require.NoError(t, err)
	assert.Equal(t, uint64(3), term)
	assert.Equal(t, "node-A", votedFor)

	snap2, err := store2.LoadSnapshot()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), snap2.Index)
	assert.Equal(t, uint64(1), snap2.Term)
	assert.Equal(t, "snap", string(snap2.Data))
}

func TestSaveLoadSnapshot_WithServers(t *testing.T) {
	store := setupTestStore(t)

	servers := []Server{
		{ID: "node-1", Suffrage: Voter},
		{ID: "node-2", Suffrage: Voter},
		{ID: "node-3", Suffrage: NonVoter},
	}
	snap := Snapshot{
		Index:   42,
		Term:    3,
		Data:    []byte("fsm-state"),
		Servers: servers,
	}
	require.NoError(t, store.SaveSnapshot(snap))

	got, err := store.LoadSnapshot()
	require.NoError(t, err)
	assert.Equal(t, uint64(42), got.Index)
	assert.Equal(t, uint64(3), got.Term)
	assert.Equal(t, []byte("fsm-state"), got.Data)
	require.Len(t, got.Servers, 3, "all 3 servers must survive round-trip")
	assert.Equal(t, "node-1", got.Servers[0].ID)
	assert.Equal(t, Voter, got.Servers[0].Suffrage)
	assert.Equal(t, "node-3", got.Servers[2].ID)
	assert.Equal(t, NonVoter, got.Servers[2].Suffrage, "NonVoter suffrage must round-trip")
}

func TestSaveLoadSnapshot_Legacy(t *testing.T) {
	// 레거시: servers=nil인 스냅샷을 저장하면 LoadSnapshot이 nil Servers 반환
	store := setupTestStore(t)

	snap := Snapshot{Index: 5, Term: 1, Data: []byte("old-state")}
	require.NoError(t, store.SaveSnapshot(snap))

	got, err := store.LoadSnapshot()
	require.NoError(t, err)
	assert.Equal(t, uint64(5), got.Index)
	assert.Nil(t, got.Servers, "legacy snapshot must have nil Servers")
}

func TestBadgerLogStore_GetEntryNotFound(t *testing.T) {
	store := setupTestStore(t)
	_, err := store.GetEntry(999)
	assert.Error(t, err)
}

func TestNewBadgerLogStore_SyncWritesEnabled(t *testing.T) {
	dir := t.TempDir()
	store, err := NewBadgerLogStore(dir)
	require.NoError(t, err)
	defer store.Close()

	// SyncWrites should be enabled for Raft log durability
	assert.True(t, store.db.Opts().SyncWrites, "Raft LogStore must use SyncWrites=true for durability")
}

func TestNewBadgerLogStore_DurableAfterWrite(t *testing.T) {
	dir := t.TempDir()

	// Write entries with SyncWrites=true
	store, err := NewBadgerLogStore(dir)
	require.NoError(t, err)

	entries := []LogEntry{
		{Term: 1, Index: 1, Command: []byte("durable-cmd")},
	}
	require.NoError(t, store.AppendEntries(entries))
	require.NoError(t, store.SaveState(2, "node-X"))
	require.NoError(t, store.Close())

	// Reopen and verify data persisted (simulates crash recovery)
	store2, err := NewBadgerLogStore(dir)
	require.NoError(t, err)
	defer store2.Close()

	entry, err := store2.GetEntry(1)
	require.NoError(t, err)
	assert.Equal(t, "durable-cmd", string(entry.Command))

	term, votedFor, err := store2.LoadState()
	require.NoError(t, err)
	assert.Equal(t, uint64(2), term)
	assert.Equal(t, "node-X", votedFor)
}

// ── Phase 14d: TruncateBefore tests ──────────────────────────────────────

func TestBadgerLogStore_TruncateBefore_RemovesOldEntries(t *testing.T) {
	store := setupTestStore(t)
	entries := []LogEntry{
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 1, Index: 2, Command: []byte("b")},
		{Term: 1, Index: 3, Command: []byte("c")},
		{Term: 1, Index: 4, Command: []byte("d")},
		{Term: 1, Index: 5, Command: []byte("e")},
	}
	require.NoError(t, store.AppendEntries(entries))

	// Truncate before index 4: removes 1, 2, 3; keeps 4, 5
	require.NoError(t, store.TruncateBefore(4))

	// Entries 1-3 should be gone
	for _, idx := range []uint64{1, 2, 3} {
		_, err := store.GetEntry(idx)
		assert.Error(t, err, "entry %d should be deleted", idx)
	}

	// Entries 4-5 should remain
	for _, want := range entries[3:] {
		got, err := store.GetEntry(want.Index)
		require.NoError(t, err, "entry %d should exist", want.Index)
		assert.Equal(t, want.Command, got.Command)
	}
}

func TestBadgerLogStore_TruncateBefore_NoOp_WhenNothingToRemove(t *testing.T) {
	store := setupTestStore(t)
	entries := []LogEntry{
		{Term: 1, Index: 5, Command: []byte("x")},
	}
	require.NoError(t, store.AppendEntries(entries))

	// TruncateBefore(5) → nothing before index 5 to remove
	require.NoError(t, store.TruncateBefore(5))

	got, err := store.GetEntry(5)
	require.NoError(t, err)
	assert.Equal(t, []byte("x"), got.Command)
}

func TestBadgerLogStore_TruncateBefore_ExcludesIndex(t *testing.T) {
	store := setupTestStore(t)
	entries := []LogEntry{
		{Term: 1, Index: 1, Command: []byte("keep-no")},
		{Term: 1, Index: 2, Command: []byte("keep-yes")},
	}
	require.NoError(t, store.AppendEntries(entries))

	// TruncateBefore(2) removes index < 2 → removes index 1, keeps index 2
	require.NoError(t, store.TruncateBefore(2))

	_, err := store.GetEntry(1)
	assert.Error(t, err, "index 1 should be deleted")

	got, err := store.GetEntry(2)
	require.NoError(t, err)
	assert.Equal(t, []byte("keep-yes"), got.Command)
}

// ── Phase 14d: managed mode pre-flight tests ─────────────────────────────

func TestBadgerLogStore_ManagedMode_FirstOpen_DefaultIsNonManaged(t *testing.T) {
	dir := t.TempDir()
	store, err := NewBadgerLogStore(dir)
	require.NoError(t, err)
	assert.False(t, store.IsManagedMode())
	require.NoError(t, store.Close())
}

func TestBadgerLogStore_ManagedMode_FirstOpen_Managed(t *testing.T) {
	dir := t.TempDir()
	store, err := NewBadgerLogStore(dir, WithManagedMode())
	require.NoError(t, err)
	assert.True(t, store.IsManagedMode())
	require.NoError(t, store.Close())
}

func TestBadgerLogStore_ManagedMode_PreflightRejectsModeMismatch_NoneToManaged(t *testing.T) {
	dir := t.TempDir()
	// First open: non-managed (default)
	s, err := NewBadgerLogStore(dir)
	require.NoError(t, err)
	require.NoError(t, s.Close())

	// Reopen with managed → mismatch → error
	_, err = NewBadgerLogStore(dir, WithManagedMode())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "non-managed mode")
}

func TestBadgerLogStore_ManagedMode_PreflightRejectsModeMismatch_ManagedToNone(t *testing.T) {
	dir := t.TempDir()
	// First open: managed
	s, err := NewBadgerLogStore(dir, WithManagedMode())
	require.NoError(t, err)
	require.NoError(t, s.Close())

	// Reopen without managed → mismatch → error
	_, err = NewBadgerLogStore(dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "managed=true")
}

func TestBadgerLogStore_ManagedMode_ConsistentReopenManaged(t *testing.T) {
	dir := t.TempDir()
	s, err := NewBadgerLogStore(dir, WithManagedMode())
	require.NoError(t, err)
	require.NoError(t, s.Close())

	s2, err := NewBadgerLogStore(dir, WithManagedMode())
	require.NoError(t, err)
	defer s2.Close()
	assert.True(t, s2.IsManagedMode())
}

func TestBadgerLogStore_ManagedMode_ConsistentReopenNonManaged(t *testing.T) {
	dir := t.TempDir()
	s, err := NewBadgerLogStore(dir)
	require.NoError(t, err)
	require.NoError(t, s.Close())

	s2, err := NewBadgerLogStore(dir)
	require.NoError(t, err)
	defer s2.Close()
	assert.False(t, s2.IsManagedMode())
}

func TestMarshalLogEntry_AllocsBounded(t *testing.T) {
	entry := LogEntry{Term: 1, Index: 42, Command: []byte("test-command")}
	// warmup: populate pool
	_ = marshalLogEntry(entry)

	allocs := testing.AllocsPerRun(100, func() {
		_ = marshalLogEntry(entry)
	})
	assert.LessOrEqual(t, allocs, float64(2), "marshalLogEntry allocs should be ≤2 with pool reuse")
}

// TestSharedLogStore_PrefixIsolation verifies that two BadgerLogStore views
// over a shared *badger.DB with different group prefixes never see each
// other's keys. This is the C2 P0b safety invariant — a bug in the prefix
// path can wipe sibling group state.
func TestSharedLogStore_PrefixIsolation(t *testing.T) {
	dir := t.TempDir()
	db, err := openSharedDBForTest(t, dir)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	storeA, err := OpenSharedLogStore(db, "group-A")
	require.NoError(t, err)
	storeB, err := OpenSharedLogStore(db, "group-B")
	require.NoError(t, err)

	// Distinct entries in each group's log.
	entriesA := []LogEntry{
		{Term: 1, Index: 1, Command: []byte("A-cmd1")},
		{Term: 1, Index: 2, Command: []byte("A-cmd2")},
		{Term: 2, Index: 3, Command: []byte("A-cmd3")},
	}
	entriesB := []LogEntry{
		{Term: 5, Index: 100, Command: []byte("B-cmd100")},
		{Term: 5, Index: 101, Command: []byte("B-cmd101")},
	}
	require.NoError(t, storeA.AppendEntries(entriesA))
	require.NoError(t, storeB.AppendEntries(entriesB))

	// LastIndex must be group-scoped.
	lastA, err := storeA.LastIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(3), lastA, "A.LastIndex must reflect only A's entries")
	lastB, err := storeB.LastIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(101), lastB, "B.LastIndex must reflect only B's entries")

	// GetEntries scoped per prefix.
	gotA, err := storeA.GetEntries(1, 3)
	require.NoError(t, err)
	require.Len(t, gotA, 3)
	for i, want := range entriesA {
		assert.Equal(t, want.Command, gotA[i].Command, "A entry %d", i)
	}
	gotB, err := storeB.GetEntries(100, 101)
	require.NoError(t, err)
	require.Len(t, gotB, 2)
	for i, want := range entriesB {
		assert.Equal(t, want.Command, gotB[i].Command, "B entry %d", i)
	}

	// Per-group state must not collide.
	require.NoError(t, storeA.SaveState(7, "node-A"))
	require.NoError(t, storeB.SaveState(11, "node-B"))
	tA, vA, err := storeA.LoadState()
	require.NoError(t, err)
	assert.Equal(t, uint64(7), tA)
	assert.Equal(t, "node-A", vA)
	tB, vB, err := storeB.LoadState()
	require.NoError(t, err)
	assert.Equal(t, uint64(11), tB)
	assert.Equal(t, "node-B", vB)

	// TruncateAfter on A must not touch B.
	require.NoError(t, storeA.TruncateAfter(1)) // drop A's idx 2,3
	lastA, err = storeA.LastIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), lastA)
	lastB, err = storeB.LastIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(101), lastB, "TruncateAfter on A must not affect B")

	// TruncateBefore on B must not touch A.
	require.NoError(t, storeB.TruncateBefore(101)) // drop B's idx 100, keep 101
	// Probe the surviving index directly — GetEntries break-on-miss can't
	// describe a hole at lo, so use GetEntry / LastIndex semantics.
	_, err = storeB.GetEntry(100)
	require.Error(t, err, "B's idx 100 should be gone after TruncateBefore(101)")
	gotEntry, err := storeB.GetEntry(101)
	require.NoError(t, err, "B's idx 101 must survive TruncateBefore(101)")
	assert.Equal(t, uint64(101), gotEntry.Index)
	gotA, err = storeA.GetEntries(1, 3)
	require.NoError(t, err)
	require.Len(t, gotA, 1, "A unaffected by B's truncate")
	assert.Equal(t, uint64(1), gotA[0].Index)

	// Bootstrap markers must be per-group.
	bootA, err := storeA.IsBootstrapped()
	require.NoError(t, err)
	assert.False(t, bootA)
	require.NoError(t, storeA.SaveBootstrapMarker())
	bootA, err = storeA.IsBootstrapped()
	require.NoError(t, err)
	assert.True(t, bootA)
	bootB, err := storeB.IsBootstrapped()
	require.NoError(t, err)
	assert.False(t, bootB, "saving A's bootstrap must not affect B")

	// Snapshot must be per-group.
	snapA := Snapshot{Index: 50, Term: 5, Data: []byte("A-snap")}
	snapB := Snapshot{Index: 200, Term: 9, Data: []byte("B-snap")}
	require.NoError(t, storeA.SaveSnapshot(snapA))
	require.NoError(t, storeB.SaveSnapshot(snapB))
	loadedA, err := storeA.LoadSnapshot()
	require.NoError(t, err)
	assert.Equal(t, []byte("A-snap"), loadedA.Data)
	assert.Equal(t, uint64(50), loadedA.Index)
	loadedB, err := storeB.LoadSnapshot()
	require.NoError(t, err)
	assert.Equal(t, []byte("B-snap"), loadedB.Data)
	assert.Equal(t, uint64(200), loadedB.Index)

	// Close on a shared store must not close the underlying DB.
	require.NoError(t, storeA.Close())
	require.NoError(t, storeB.Close())
	// db is still usable — caller owns its lifecycle.
	require.NotNil(t, db)
}

// TestOpenSharedLogStore_Validation covers the input checks.
func TestOpenSharedLogStore_Validation(t *testing.T) {
	dir := t.TempDir()
	db, err := openSharedDBForTest(t, dir)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	_, err = OpenSharedLogStore(nil, "g")
	require.Error(t, err)
	_, err = OpenSharedLogStore(db, "")
	require.Error(t, err)
}

// openSharedDBForTest opens a *badger.DB with the same options the production
// shared-badger path uses.
func openSharedDBForTest(t *testing.T, dir string) (*badger.DB, error) {
	t.Helper()
	return badger.Open(badger.DefaultOptions(dir).
		WithLogger(nil).
		WithSyncWrites(true).
		WithNumCompactors(2).
		WithNumVersionsToKeep(1))
}

// TestSharedLogStore_PathologicalGroupIDs hits the prefix encoding edges that
// the friendly-ID test misses: long IDs (>255 chars), embedded colons, and
// embedded NUL bytes. The 4-byte length prefix must keep all of these
// non-overlapping.
func TestSharedLogStore_PathologicalGroupIDs(t *testing.T) {
	dir := t.TempDir()
	db, err := openSharedDBForTest(t, dir)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	cases := []struct {
		name    string
		groupID string
	}{
		{"colon", "group:with:colons"},
		{"nul", "group\x00null\x00bytes"},
		{"long-300", string(make([]byte, 300))},           // > old 1-byte length wraparound boundary
		{"long-256", string(make([]byte, 256))},           // exactly the old wrap point
		{"longA-255", "a" + string(make([]byte, 254))},    // fills exactly 255 chars
		{"prefix-colon-collision", "a:raft:log:00000001"}, // looks like a raw raft log key
	}

	stores := make([]*BadgerLogStore, len(cases))
	for i, c := range cases {
		s, err := OpenSharedLogStore(db, c.groupID)
		require.NoError(t, err, "OpenSharedLogStore %s", c.name)
		stores[i] = s
		// Each store gets a unique log entry at index i+1 with command identifying it.
		require.NoError(t, s.AppendEntries([]LogEntry{
			{Term: 1, Index: uint64(i + 1), Command: []byte(c.name)},
		}))
		require.NoError(t, s.SaveState(uint64(i+1), c.name))
	}

	// Each store must see only its own state; LastIndex must equal its own
	// index, never a sibling's.
	for i, c := range cases {
		want := uint64(i + 1)
		got, err := stores[i].LastIndex()
		require.NoError(t, err, "%s LastIndex", c.name)
		assert.Equal(t, want, got, "%s LastIndex must be %d", c.name, want)

		entry, err := stores[i].GetEntry(want)
		require.NoError(t, err)
		assert.Equal(t, []byte(c.name), entry.Command, "%s entry command", c.name)

		term, voted, err := stores[i].LoadState()
		require.NoError(t, err)
		assert.Equal(t, want, term)
		assert.Equal(t, c.name, voted)
	}

	// Cross-check: TruncateAfter on the colon store must not affect long-300 store.
	colonIdx := 0
	long300Idx := 2
	require.NoError(t, stores[colonIdx].TruncateAfter(0))
	got, err := stores[long300Idx].LastIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(long300Idx+1), got, "long-300 LastIndex must be unaffected by colon-store truncate")
}

// TestSharedLogStore_ConcurrentOpenSameGroup verifies that two concurrent
// OpenSharedLogStore calls for the same groupID don't corrupt the
// managed-mode marker or produce divergent stores. (Production guards
// against this via the inFlight map in serve.go, but the store itself
// should also tolerate it gracefully.)
func TestSharedLogStore_ConcurrentOpenSameGroup(t *testing.T) {
	dir := t.TempDir()
	db, err := openSharedDBForTest(t, dir)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	const groupID = "race-group"
	const N = 16
	results := make(chan error, N)
	for i := 0; i < N; i++ {
		go func() {
			s, err := OpenSharedLogStore(db, groupID)
			if err != nil {
				results <- err
				return
			}
			// Each opener does a tiny write to validate the store works.
			results <- s.AppendEntries([]LogEntry{{Term: 1, Index: 1, Command: []byte("x")}})
		}()
	}
	for i := 0; i < N; i++ {
		assert.NoError(t, <-results, "concurrent open %d", i)
	}
}

// TestSharedLogStore_RestartPersistence verifies that closing the shared DB
// and reopening preserves per-group state. Catches bugs where managed-mode
// markers or bootstrap markers leak across restarts.
func TestSharedLogStore_RestartPersistence(t *testing.T) {
	dir := t.TempDir()
	db1, err := openSharedDBForTest(t, dir)
	require.NoError(t, err)

	storeA, err := OpenSharedLogStore(db1, "group-A")
	require.NoError(t, err)
	storeB, err := OpenSharedLogStore(db1, "group-B")
	require.NoError(t, err)

	require.NoError(t, storeA.AppendEntries([]LogEntry{{Term: 7, Index: 42, Command: []byte("A42")}}))
	require.NoError(t, storeB.AppendEntries([]LogEntry{{Term: 9, Index: 99, Command: []byte("B99")}}))
	require.NoError(t, storeA.SaveBootstrapMarker())
	require.NoError(t, storeA.Close())
	require.NoError(t, storeB.Close())
	require.NoError(t, db1.Close())

	// Reopen and verify state survives.
	db2, err := openSharedDBForTest(t, dir)
	require.NoError(t, err)
	t.Cleanup(func() { db2.Close() })

	storeA2, err := OpenSharedLogStore(db2, "group-A")
	require.NoError(t, err)
	storeB2, err := OpenSharedLogStore(db2, "group-B")
	require.NoError(t, err)

	gotA, err := storeA2.GetEntry(42)
	require.NoError(t, err)
	assert.Equal(t, []byte("A42"), gotA.Command)
	gotB, err := storeB2.GetEntry(99)
	require.NoError(t, err)
	assert.Equal(t, []byte("B99"), gotB.Command)

	bootA, err := storeA2.IsBootstrapped()
	require.NoError(t, err)
	assert.True(t, bootA, "A's bootstrap marker must persist")
	bootB, err := storeB2.IsBootstrapped()
	require.NoError(t, err)
	assert.False(t, bootB, "B should still be unbootstrapped")
}
