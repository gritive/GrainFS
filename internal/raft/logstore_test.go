package raft

import (
	"errors"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// logStoreFactory describes a named LogStore constructor for table-driven tests.
// Both memLogStore and badgerLogStore are driven through the same scenarios.
type logStoreFactory struct {
	name string
	new  func(t *testing.T) LogStore
}

var allStores = []logStoreFactory{
	{
		name: "mem",
		new:  func(t *testing.T) LogStore { return newMemLogStore() },
	},
	{
		name: "badger",
		new: func(t *testing.T) LogStore {
			dir := t.TempDir()
			db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			store, err := newBadgerLogStore(db, []byte("raft/v2/log/"))
			require.NoError(t, err)
			return store
		},
	},
}

// TestLogStore_Empty verifies empty-store invariants.
func TestLogStore_Empty(t *testing.T) {
	for _, f := range allStores {
		t.Run(f.name, func(t *testing.T) {
			s := f.new(t)
			require.Equal(t, uint64(1), s.FirstIndex(), "FirstIndex on empty store")
			require.Equal(t, uint64(0), s.LastIndex(), "LastIndex on empty store")

			_, err := s.Entry(1)
			require.ErrorIs(t, err, ErrLogIndexOutOfRange, "Entry(1) on empty store")

			term, err := s.TermAt(0)
			require.NoError(t, err, "TermAt(0) sentinel")
			require.Equal(t, uint64(0), term, "TermAt(0) returns 0")

			_, err = s.TermAt(1)
			require.ErrorIs(t, err, ErrLogIndexOutOfRange, "TermAt(1) on empty store")

			entries, err := s.EntriesFrom(1, 10)
			require.NoError(t, err, "EntriesFrom(1) on empty store is not an error")
			require.Empty(t, entries)
		})
	}
}

// TestLogStore_AppendAndRead appends 3 entries and verifies random access.
func TestLogStore_AppendAndRead(t *testing.T) {
	for _, f := range allStores {
		t.Run(f.name, func(t *testing.T) {
			s := f.new(t)
			batch := []LogEntry{
				{Term: 1, Index: 1, Command: []byte("a")},
				{Term: 1, Index: 2, Command: []byte("b")},
				{Term: 2, Index: 3, Command: []byte("c")},
			}
			require.NoError(t, s.Append(batch))

			require.Equal(t, uint64(3), s.LastIndex())

			for _, want := range batch {
				got, err := s.Entry(want.Index)
				require.NoError(t, err, "Entry(%d)", want.Index)
				require.Equal(t, want, got, "Entry(%d)", want.Index)
			}

			term, err := s.TermAt(2)
			require.NoError(t, err)
			require.Equal(t, uint64(1), term)

			term, err = s.TermAt(3)
			require.NoError(t, err)
			require.Equal(t, uint64(2), term)
		})
	}
}

// TestLogStore_TruncateAfter verifies TruncateAfter semantics.
func TestLogStore_TruncateAfter(t *testing.T) {
	for _, f := range allStores {
		t.Run(f.name, func(t *testing.T) {
			s := f.new(t)
			require.NoError(t, s.Append([]LogEntry{
				{Term: 1, Index: 1},
				{Term: 1, Index: 2},
				{Term: 1, Index: 3},
			}))

			// Truncate to index 2: entry at 3 gone.
			require.NoError(t, s.TruncateAfter(2))
			require.Equal(t, uint64(2), s.LastIndex())
			_, err := s.Entry(3)
			require.ErrorIs(t, err, ErrLogIndexOutOfRange, "entry 3 removed after TruncateAfter(2)")

			// Truncate to 0: store is empty.
			require.NoError(t, s.TruncateAfter(0))
			require.Equal(t, uint64(0), s.LastIndex())

			// No-op truncation on empty store.
			require.NoError(t, s.TruncateAfter(5))
			require.Equal(t, uint64(0), s.LastIndex())
		})
	}
}

// TestLogStore_EntriesFrom exercises range reads and cap.
func TestLogStore_EntriesFrom(t *testing.T) {
	for _, f := range allStores {
		t.Run(f.name, func(t *testing.T) {
			s := f.new(t)
			require.NoError(t, s.Append([]LogEntry{
				{Term: 1, Index: 1},
				{Term: 1, Index: 2},
				{Term: 1, Index: 3},
				{Term: 2, Index: 4},
			}))

			// Uncapped: all entries from index 2.
			got, err := s.EntriesFrom(2, 0)
			require.NoError(t, err)
			require.Len(t, got, 3)
			require.Equal(t, uint64(2), got[0].Index)
			require.Equal(t, uint64(4), got[2].Index)

			// Capped at 1.
			got, err = s.EntriesFrom(2, 1)
			require.NoError(t, err)
			require.Len(t, got, 1)
			require.Equal(t, uint64(2), got[0].Index)

			// Out of range.
			_, err = s.EntriesFrom(10, 10)
			require.ErrorIs(t, err, ErrLogIndexOutOfRange)
		})
	}
}

// TestLogStore_EntriesFromReturnsCopy verifies the returned slice does not
// alias internal storage — mutations to the returned slice must not affect
// subsequent reads from the store.
func TestLogStore_EntriesFromReturnsCopy(t *testing.T) {
	for _, f := range allStores {
		t.Run(f.name, func(t *testing.T) {
			s := f.new(t)
			require.NoError(t, s.Append([]LogEntry{
				{Term: 1, Index: 1, Command: []byte("original")},
			}))

			got, err := s.EntriesFrom(1, 10)
			require.NoError(t, err)
			require.Len(t, got, 1)

			// Mutate the returned slice — both a scalar field and the Command byte slice.
			// The byte-slice mutation is the real aliasing risk: a shallow struct copy
			// shares the underlying []byte header.
			got[0].Term = 99
			got[0].Command[0] = 'X'

			// Store must be unaffected on both axes.
			entry, err := s.Entry(1)
			require.NoError(t, err)
			require.Equal(t, uint64(1), entry.Term, "store must not reflect scalar mutation of returned slice")
			require.Equal(t, byte('o'), entry.Command[0], "store must not reflect Command byte mutation")
		})
	}
}

// TestLogStore_Append_NonContiguous verifies that Append panics when the
// first entry's Index does not immediately follow LastIndex.
func TestLogStore_Append_NonContiguous(t *testing.T) {
	for _, f := range allStores {
		t.Run(f.name, func(t *testing.T) {
			s := f.new(t)
			require.NoError(t, s.Append([]LogEntry{{Term: 1, Index: 1}}))

			require.PanicsWithValue(t, "raftv2: Append: non-contiguous index", func() {
				// Index 3 is not contiguous after LastIndex()==1.
				_ = s.Append([]LogEntry{{Term: 1, Index: 3}})
			}, "non-contiguous Append must panic with the documented message")
		})
	}
}

// TestLogStore_TruncateThenAppend exercises the conflict-resolution sequence
// the actor follows: TruncateAfter on conflict, then Append the new suffix.
// Catches grow-from-truncated slice aliasing bugs (capacity/length confusion).
func TestLogStore_TruncateThenAppend(t *testing.T) {
	for _, f := range allStores {
		t.Run(f.name, func(t *testing.T) {
			s := f.new(t)
			require.NoError(t, s.Append([]LogEntry{
				{Term: 1, Index: 1},
				{Term: 1, Index: 2},
				{Term: 1, Index: 3},
			}))

			require.NoError(t, s.TruncateAfter(2))
			require.Equal(t, uint64(2), s.LastIndex())

			// Append a new entry at index 3 (replacing the truncated one).
			require.NoError(t, s.Append([]LogEntry{{Term: 2, Index: 3, Command: []byte("new")}}))

			got, err := s.Entry(3)
			require.NoError(t, err)
			require.Equal(t, uint64(2), got.Term, "term reflects new appended entry, not pre-truncate value")
			require.Equal(t, []byte("new"), got.Command)
		})
	}
}

// TestLogStore_Entry_OutOfRange verifies ErrLogIndexOutOfRange on 0 and beyond LastIndex.
func TestLogStore_Entry_OutOfRange(t *testing.T) {
	for _, f := range allStores {
		t.Run(f.name, func(t *testing.T) {
			s := f.new(t)
			require.NoError(t, s.Append([]LogEntry{{Term: 1, Index: 1}}))

			_, err := s.Entry(0)
			require.True(t, errors.Is(err, ErrLogIndexOutOfRange), "Entry(0) must error")

			_, err = s.Entry(2)
			require.True(t, errors.Is(err, ErrLogIndexOutOfRange), "Entry(2) beyond LastIndex must error")
		})
	}
}

// TestLogStore_CompactBefore exercises the snapshot-driven compaction path
// for both memLogStore and badgerLogStore. After CompactBefore(N):
//   - FirstIndex() == N+1
//   - Entries 1..N return ErrLogIndexOutOfRange
//   - Entries N+1..last still readable
//   - TermAt(N) returns the snapshot's prevTerm (boundary fast path)
//   - TermAt(<N) returns ErrLogIndexOutOfRange
//   - Append at N+last+1 still works (LastIndex grows past the boundary)
//   - EntriesFrom with startIdx < FirstIndex returns ErrLogIndexOutOfRange
func TestLogStore_CompactBefore(t *testing.T) {
	for _, f := range allStores {
		t.Run(f.name, func(t *testing.T) {
			s := f.new(t)
			// Seed indices 1..10, with mixed terms so TermAt(boundary) is
			// distinguishable from neighbours.
			for i := uint64(1); i <= 10; i++ {
				term := uint64(1)
				if i >= 6 {
					term = 2
				}
				require.NoError(t, s.Append([]LogEntry{{Term: term, Index: i}}))
			}

			// Compact through index 5. Entry at idx 5 has Term=1 → prevTerm=1.
			require.NoError(t, s.CompactBefore(5))

			require.Equal(t, uint64(6), s.FirstIndex(), "FirstIndex must advance to 6")
			require.Equal(t, uint64(10), s.LastIndex(), "LastIndex must remain 10")

			for i := uint64(1); i <= 5; i++ {
				_, err := s.Entry(i)
				require.ErrorIs(t, err, ErrLogIndexOutOfRange, "Entry(%d) must be compacted", i)
			}
			for i := uint64(6); i <= 10; i++ {
				e, err := s.Entry(i)
				require.NoError(t, err, "Entry(%d) must still be present", i)
				require.Equal(t, i, e.Index)
			}

			// TermAt(5) — boundary fast path — returns prevTerm (=1, the
			// term of the now-discarded entry at idx 5).
			term, err := s.TermAt(5)
			require.NoError(t, err)
			require.Equal(t, uint64(1), term, "TermAt(boundary) returns prevTerm")

			// TermAt(4) — strictly below boundary — errors.
			_, err = s.TermAt(4)
			require.ErrorIs(t, err, ErrLogIndexOutOfRange)

			// EntriesFrom below FirstIndex must error.
			_, err = s.EntriesFrom(5, 10)
			require.ErrorIs(t, err, ErrLogIndexOutOfRange,
				"EntriesFrom(<FirstIndex) must reject")

			// Append continues working past LastIndex.
			require.NoError(t, s.Append([]LogEntry{{Term: 3, Index: 11}}))
			require.Equal(t, uint64(11), s.LastIndex())
			e, err := s.Entry(11)
			require.NoError(t, err)
			require.Equal(t, uint64(3), e.Term)
		})
	}
}

// TestLogStore_CompactBefore_Idempotent: calling CompactBefore at or below
// the current FirstIndex is a no-op.
func TestLogStore_CompactBefore_Idempotent(t *testing.T) {
	for _, f := range allStores {
		t.Run(f.name, func(t *testing.T) {
			s := f.new(t)
			for i := uint64(1); i <= 5; i++ {
				require.NoError(t, s.Append([]LogEntry{{Term: 1, Index: i}}))
			}
			require.NoError(t, s.CompactBefore(3))
			require.Equal(t, uint64(4), s.FirstIndex())
			// Re-compact at a lower boundary (already covered) — no-op.
			require.NoError(t, s.CompactBefore(2))
			require.Equal(t, uint64(4), s.FirstIndex(), "FirstIndex must not regress")
		})
	}
}

func TestLogStore_InstallSnapshotBoundaryAfterPriorSnapshot(t *testing.T) {
	for _, f := range allStores {
		t.Run(f.name, func(t *testing.T) {
			s := f.new(t)
			installer, ok := s.(snapshotInstaller)
			require.True(t, ok)

			require.NoError(t, installer.InstallSnapshotBoundary(50, 1))
			require.Equal(t, uint64(51), s.FirstIndex())
			require.Equal(t, uint64(50), s.LastIndex())

			require.NoError(t, s.Append([]LogEntry{{Term: 1, Index: 51}}))
			require.NoError(t, s.TruncateAfter(s.FirstIndex()-1))
			require.NoError(t, installer.InstallSnapshotBoundary(100, 2))
			require.Equal(t, uint64(101), s.FirstIndex())
			require.Equal(t, uint64(100), s.LastIndex())
			term, err := s.TermAt(100)
			require.NoError(t, err)
			require.Equal(t, uint64(2), term)
		})
	}
}

// TestLogStore_CompactBefore_BeyondLast: CompactBefore at boundary > LastIndex
// returns ErrLogIndexOutOfRange (no entry covers that index).
func TestLogStore_CompactBefore_BeyondLast(t *testing.T) {
	for _, f := range allStores {
		t.Run(f.name, func(t *testing.T) {
			s := f.new(t)
			require.NoError(t, s.Append([]LogEntry{{Term: 1, Index: 1}}))
			err := s.CompactBefore(99)
			require.ErrorIs(t, err, ErrLogIndexOutOfRange)
		})
	}
}
