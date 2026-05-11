package dedup

import (
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func newTestBadger(t *testing.T) *badger.DB {
	t.Helper()
	opts := badger.DefaultOptions("").WithInMemory(true).WithLogger(nil)
	db, err := badger.Open(opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func readRefcount(t *testing.T, idx *badgerIndex, key string) int32 {
	t.Helper()
	var rc int32
	require.NoError(t, idx.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(refPrefix + key))
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error {
			rc, _ = decodeRefVal(v)
			return nil
		})
	}))
	return rc
}

func testMeta(snapID string) SnapshotMeta {
	return SnapshotMeta{
		SnapID:     snapID,
		CreatedAt:  time.Unix(1_700_000_000, 0).UTC(),
		BlockCount: 1,
	}
}

func TestSnapshotBeginCommitIdempotent(t *testing.T) {
	idx := NewBadgerIndex(newTestBadger(t)).(*badgerIndex)
	require.NoError(t, idx.SnapshotBegin("v", "s1"))
	require.NoError(t, idx.SnapshotBegin("v", "s1")) // idempotent
	require.NoError(t, idx.SnapshotCommit("v", "s1", testMeta("s1")))
	require.NoError(t, idx.SnapshotCommit("v", "s1", testMeta("s1"))) // idempotent

	// SnapshotCommit on non-existent → error
	require.Error(t, idx.SnapshotCommit("v", "no-such", testMeta("no-such")))
}

func TestSnapshotAppendChunkIncRefs(t *testing.T) {
	idx := NewBadgerIndex(newTestBadger(t)).(*badgerIndex)
	// Seed: WriteBlock("v", 0) → canonical "key0"
	var h0 [32]byte
	h0[0] = 1
	res, err := idx.WriteBlock("v", 0, h0, "key0")
	require.NoError(t, err)
	require.True(t, res.IsNew)

	require.NoError(t, idx.SnapshotBegin("v", "s1"))
	require.NoError(t, idx.SnapshotAppendChunk("v", "s1", []SnapshotBlockEntry{{BlkNum: 0, Canonical: "key0"}}))
	require.NoError(t, idx.SnapshotCommit("v", "s1", testMeta("s1")))

	// Refcount on "key0" must be 2 (live + snap).
	rc := readRefcount(t, idx, "key0")
	require.Equal(t, int32(2), rc)
}

// R6: invariant test
// TestWriteBlockDoesNotDeleteSnapshotPinnedCanonical asserts the load-bearing
// invariant of the (α) refcount-shared design: when a snapshot holds refcount
// on a canonical, a subsequent live overwrite must leave ToDelete empty.
func TestWriteBlockDoesNotDeleteSnapshotPinnedCanonical(t *testing.T) {
	idx := NewBadgerIndex(newTestBadger(t)).(*badgerIndex)
	var hA, hB [32]byte
	hA[0] = 1
	hB[0] = 2
	_, err := idx.WriteBlock("v", 0, hA, "kA")
	require.NoError(t, err)
	require.NoError(t, idx.SnapshotBegin("v", "s1"))
	require.NoError(t, idx.SnapshotAppendChunk("v", "s1", []SnapshotBlockEntry{{BlkNum: 0, Canonical: "kA"}}))
	require.NoError(t, idx.SnapshotCommit("v", "s1", testMeta("s1")))

	res, err := idx.WriteBlock("v", 0, hB, "kB")
	require.NoError(t, err)
	require.Equal(t, "kB", res.Canonical)
	require.True(t, res.IsNew)
	require.Empty(t, res.ToDelete, "kA must NOT be marked for delete while snapshot holds ref")
	require.Equal(t, int32(1), readRefcount(t, idx, "kA"))
}
