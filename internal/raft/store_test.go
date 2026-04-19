package raft

import (
	"testing"

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
	idx, term, data, err := store.LoadSnapshot()
	require.NoError(t, err)
	assert.Equal(t, uint64(0), idx)
	assert.Equal(t, uint64(0), term)
	assert.Nil(t, data)

	// Save snapshot
	snapData := []byte(`{"state":"snapshot-data"}`)
	require.NoError(t, store.SaveSnapshot(10, 3, snapData))

	idx, term, data, err = store.LoadSnapshot()
	require.NoError(t, err)
	assert.Equal(t, uint64(10), idx)
	assert.Equal(t, uint64(3), term)
	assert.Equal(t, snapData, data)
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
	require.NoError(t, store1.SaveSnapshot(1, 1, []byte("snap")))
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

	idx, snapTerm, data, err := store2.LoadSnapshot()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), idx)
	assert.Equal(t, uint64(1), snapTerm)
	assert.Equal(t, "snap", string(data))
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
