package raftv2

import (
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// openBadgerStore is a helper that opens a badgerLogStore in dir and registers
// db.Close() as a test cleanup action.
func openBadgerStore(t *testing.T, dir string) (*badgerLogStore, *badger.DB) {
	t.Helper()
	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	store, err := newBadgerLogStore(db, []byte("raft/v2/log/"))
	require.NoError(t, err)
	return store, db
}

// TestBadgerLogStore_SchemaVersionRefusesPreM60 simulates a pre-M6.0 store
// (no schema-version stamp but with entries / compaction meta present) and
// confirms newBadgerLogStore refuses to open it.
func TestBadgerLogStore_SchemaVersionRefusesPreM60(t *testing.T) {
	dir := t.TempDir()
	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	require.NoError(t, err)
	// Seed a fake entry key under the expected prefix without the schema
	// stamp. The shape mirrors what a pre-M6.0 store would have on disk.
	prefix := []byte("raft/v2/log/")
	key := make([]byte, len(prefix)+8)
	copy(key, prefix)
	for i := len(prefix); i < len(key); i++ {
		key[i] = 0
	}
	key[len(key)-1] = 1
	require.NoError(t, db.Update(func(txn *badger.Txn) error {
		// 21 bytes of zeroes — short payload, but the existence of any
		// entry-shaped key is enough to trip the gate.
		return txn.Set(key, make([]byte, 21))
	}))
	require.NoError(t, db.Close())

	db2, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db2.Close() })
	_, err = newBadgerLogStore(db2, prefix)
	require.Error(t, err)
	require.Contains(t, err.Error(), "schema version")
}

// TestBadgerLogStore_SchemaVersionStampedOnFreshOpen confirms an empty
// directory gets the schema stamp on first open and survives a reopen.
func TestBadgerLogStore_SchemaVersionStampedOnFreshOpen(t *testing.T) {
	dir := t.TempDir()
	store, db := openBadgerStore(t, dir)
	require.NoError(t, store.Append([]LogEntry{{Term: 1, Index: 1, Command: []byte("x")}}))
	require.NoError(t, db.Close())

	db2, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db2.Close() })
	store2, err := newBadgerLogStore(db2, []byte("raft/v2/log/"))
	require.NoError(t, err)
	require.Equal(t, uint64(1), store2.LastIndex())
}

// TestBadgerLogStore_EmptyDirectoryFirstOpen verifies that a fresh Badger
// directory starts with FirstIndex==1 and LastIndex==0, and that an immediate
// Append works correctly.
func TestBadgerLogStore_EmptyDirectoryFirstOpen(t *testing.T) {
	dir := t.TempDir()
	store, _ := openBadgerStore(t, dir)

	require.Equal(t, uint64(1), store.FirstIndex())
	require.Equal(t, uint64(0), store.LastIndex())

	require.NoError(t, store.Append([]LogEntry{
		{Term: 1, Index: 1, Command: []byte("hello")},
	}))
	require.Equal(t, uint64(1), store.LastIndex())
}

// TestBadgerLogStore_PersistsAcrossReopen writes entries, closes the DB, reopens
// it with the same directory, and verifies that LastIndex, Entry, and TermAt all
// reflect the durable state.
func TestBadgerLogStore_PersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: write entries and close.
	func() {
		db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
		require.NoError(t, err)
		defer db.Close()

		store, err := newBadgerLogStore(db, []byte("raft/v2/log/"))
		require.NoError(t, err)

		require.NoError(t, store.Append([]LogEntry{
			{Term: 1, Index: 1, Command: []byte("first")},
			{Term: 1, Index: 2, Command: []byte("second")},
			{Term: 2, Index: 3, Command: []byte("third")},
		}))
	}()

	// Phase 2: reopen with the same dir and verify durability.
	db2, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db2.Close() })

	store2, err := newBadgerLogStore(db2, []byte("raft/v2/log/"))
	require.NoError(t, err)

	require.Equal(t, uint64(3), store2.LastIndex(), "LastIndex must survive reopen")

	e1, err := store2.Entry(1)
	require.NoError(t, err)
	require.Equal(t, uint64(1), e1.Term)
	require.Equal(t, []byte("first"), e1.Command)

	e3, err := store2.Entry(3)
	require.NoError(t, err)
	require.Equal(t, uint64(2), e3.Term)
	require.Equal(t, []byte("third"), e3.Command)

	term, err := store2.TermAt(2)
	require.NoError(t, err)
	require.Equal(t, uint64(1), term)
}

// TestBadgerLogStore_TruncateAfterPersistsToDisk appends 5 entries, truncates to
// index 2, closes the DB, reopens it, and verifies that LastIndex==2 and entries
// 3-5 are gone.
func TestBadgerLogStore_TruncateAfterPersistsToDisk(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: write 5 entries, truncate to 2, close.
	func() {
		db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
		require.NoError(t, err)
		defer db.Close()

		store, err := newBadgerLogStore(db, []byte("raft/v2/log/"))
		require.NoError(t, err)

		require.NoError(t, store.Append([]LogEntry{
			{Term: 1, Index: 1},
			{Term: 1, Index: 2},
			{Term: 1, Index: 3},
			{Term: 1, Index: 4},
			{Term: 1, Index: 5},
		}))
		require.Equal(t, uint64(5), store.LastIndex())

		require.NoError(t, store.TruncateAfter(2))
		require.Equal(t, uint64(2), store.LastIndex())
	}()

	// Phase 2: reopen and verify.
	db2, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db2.Close() })

	store2, err := newBadgerLogStore(db2, []byte("raft/v2/log/"))
	require.NoError(t, err)

	require.Equal(t, uint64(2), store2.LastIndex(), "LastIndex must be 2 after truncation survives reopen")

	// Entries 1 and 2 must still be readable.
	_, err = store2.Entry(1)
	require.NoError(t, err)
	_, err = store2.Entry(2)
	require.NoError(t, err)

	// Entries 3, 4, 5 must be gone.
	for _, idx := range []uint64{3, 4, 5} {
		_, err = store2.Entry(idx)
		require.ErrorIs(t, err, ErrLogIndexOutOfRange, "entry %d must be gone after truncation", idx)
	}
}

// TestBadgerLogStore_TruncateAfter_LargeChunkedDeletes exercises the
// chunked-delete path when truncating more entries than fit in a single
// Badger transaction. With chunk size 1024, a 2500-entry truncation must
// span 3 transactions and still produce a coherent post-state.
func TestBadgerLogStore_TruncateAfter_LargeChunkedDeletes(t *testing.T) {
	dir := t.TempDir()
	store, _ := openBadgerStore(t, dir)

	// Append 2500 entries in one batch (well under txn cap for set ops).
	const total = 2500
	entries := make([]LogEntry, total)
	for i := 0; i < total; i++ {
		entries[i] = LogEntry{Term: 1, Index: uint64(i + 1)}
	}
	require.NoError(t, store.Append(entries))
	require.Equal(t, uint64(total), store.LastIndex())

	// Truncate to keep only the first 100 — forces 2400 deletes, which
	// must chunk across multiple txns.
	require.NoError(t, store.TruncateAfter(100))
	require.Equal(t, uint64(100), store.LastIndex())

	// Surviving boundary entry readable.
	e, err := store.Entry(100)
	require.NoError(t, err)
	require.Equal(t, uint64(100), e.Index)

	// Just-past boundary gone.
	_, err = store.Entry(101)
	require.ErrorIs(t, err, ErrLogIndexOutOfRange)

	// Last original entry gone.
	_, err = store.Entry(uint64(total))
	require.ErrorIs(t, err, ErrLogIndexOutOfRange)
}

// TestBadgerLogStore_RejectsCorruptHighestEntryOnOpen seeds a Badger DB with a
// key under the LogStore prefix whose value cannot be decoded as a LogEntry
// (e.g. a stale or foreign payload that the prefix happened to match). The
// constructor must refuse to open rather than seed lastIdx with garbage.
func TestBadgerLogStore_RejectsCorruptHighestEntryOnOpen(t *testing.T) {
	dir := t.TempDir()

	prefix := []byte("raft/v2/log/")
	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	// Inject a corrupt entry: well-formed key, but value is too short to decode.
	key := make([]byte, len(prefix)+8)
	copy(key, prefix)
	for i := len(prefix); i < len(key); i++ {
		key[i] = 0xFF // huge index — guaranteed to be the highest
	}
	require.NoError(t, db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, []byte("garbage")) // < entryHeaderSize bytes
	}))

	_, err = newBadgerLogStore(db, prefix)
	require.Error(t, err, "open must fail when the highest entry's value is undecodable")
}
