package dedup

import (
	"fmt"
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

func TestSnapshotAbortReleasesRefs(t *testing.T) {
	idx := NewBadgerIndex(newTestBadger(t)).(*badgerIndex)
	var h0 [32]byte
	h0[0] = 1
	_, err := idx.WriteBlock("v", 0, h0, "key0")
	require.NoError(t, err)

	require.NoError(t, idx.SnapshotBegin("v", "s-aborted"))
	require.NoError(t, idx.SnapshotAppendChunk("v", "s-aborted", []SnapshotBlockEntry{{0, "key0"}}))
	require.Equal(t, int32(2), readRefcount(t, idx, "key0"))

	toDelete, err := idx.SnapshotAbort("v", "s-aborted")
	require.NoError(t, err)
	require.Empty(t, toDelete) // refcount went 2→1, not zero
	require.Equal(t, int32(1), readRefcount(t, idx, "key0"))
	// State entry gone
	inProg, err := idx.SnapshotListInProgress()
	require.NoError(t, err)
	require.Empty(t, inProg)
}

func TestSnapshotDeleteReleasesCanonical(t *testing.T) {
	idx := NewBadgerIndex(newTestBadger(t)).(*badgerIndex)
	var h0 [32]byte
	h0[0] = 1
	_, err := idx.WriteBlock("v", 0, h0, "key0")
	require.NoError(t, err)
	require.NoError(t, idx.SnapshotBegin("v", "s1"))
	require.NoError(t, idx.SnapshotAppendChunk("v", "s1", []SnapshotBlockEntry{{0, "key0"}}))
	require.NoError(t, idx.SnapshotCommit("v", "s1", testMeta("s1")))

	// FreeBlock live → refcount 2→1 (snap still holds)
	_, shouldDel, err := idx.FreeBlock("v", 0)
	require.NoError(t, err)
	require.False(t, shouldDel)
	require.Equal(t, int32(1), readRefcount(t, idx, "key0"))

	toDelete, err := idx.SnapshotDelete("v", "s1")
	require.NoError(t, err)
	require.Equal(t, []string{"key0"}, toDelete)
	// meta should also be gone — verifiable via raw db check
	require.NoError(t, idx.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(snapMetaKey("v", "s1"))
		require.ErrorIs(t, err, badger.ErrKeyNotFound)
		return nil
	}))
}

func TestSnapshotIterAndReadBlock(t *testing.T) {
	idx := NewBadgerIndex(newTestBadger(t)).(*badgerIndex)
	require.NoError(t, idx.SnapshotBegin("v", "s1"))
	entries := []SnapshotBlockEntry{{0, "k0"}, {2, "k2"}, {5, "k5"}}
	// Seed canonicals so IncRef succeeds.
	for _, e := range entries {
		var h [32]byte
		h[0] = byte(e.BlkNum + 1)
		_, err := idx.WriteBlock("seed", e.BlkNum, h, e.Canonical)
		require.NoError(t, err)
	}
	require.NoError(t, idx.SnapshotAppendChunk("v", "s1", entries))
	require.NoError(t, idx.SnapshotCommit("v", "s1", testMeta("s1")))

	var got []SnapshotBlockEntry
	require.NoError(t, idx.SnapshotIter("v", "s1", func(b int64, c string) error {
		got = append(got, SnapshotBlockEntry{b, c})
		return nil
	}))
	require.Equal(t, entries, got) // sorted by blkNum (zero-padded key order)

	canon, found, err := idx.SnapshotReadBlock("v", "s1", 2)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "k2", canon)

	_, found, err = idx.SnapshotReadBlock("v", "s1", 99)
	require.NoError(t, err)
	require.False(t, found)
}

func TestIterLiveBlocks(t *testing.T) {
	idx := NewBadgerIndex(newTestBadger(t)).(*badgerIndex)
	// Write 3 blocks.
	for i := int64(0); i < 3; i++ {
		var h [32]byte
		h[0] = byte(i + 1)
		_, err := idx.WriteBlock("v", i, h, fmt.Sprintf("k%d", i))
		require.NoError(t, err)
	}

	var got []SnapshotBlockEntry
	require.NoError(t, idx.IterLiveBlocks("v", func(blk int64, canon string) error {
		got = append(got, SnapshotBlockEntry{blk, canon})
		return nil
	}))
	require.Equal(t, []SnapshotBlockEntry{
		{0, "k0"}, {1, "k1"}, {2, "k2"},
	}, got)

	// Other volumes are skipped.
	var h [32]byte
	h[0] = 99
	_, err := idx.WriteBlock("other", 0, h, "kOther")
	require.NoError(t, err)

	got = got[:0]
	require.NoError(t, idx.IterLiveBlocks("v", func(blk int64, canon string) error {
		got = append(got, SnapshotBlockEntry{blk, canon})
		return nil
	}))
	require.Len(t, got, 3) // unchanged — "other" volume not iterated
}

func TestSnapshotListAndPutMeta(t *testing.T) {
	idx := NewBadgerIndex(newTestBadger(t)).(*badgerIndex)

	// SnapshotList on empty vol → nil
	got, err := idx.SnapshotList("v")
	require.NoError(t, err)
	require.Empty(t, got)

	// Commit two snapshots, the second one later.
	require.NoError(t, idx.SnapshotBegin("v", "s-early"))
	require.NoError(t, idx.SnapshotCommit("v", "s-early", SnapshotMeta{
		SnapID: "s-early", CreatedAt: time.Unix(1000, 0).UTC(), BlockCount: 1,
	}))
	require.NoError(t, idx.SnapshotBegin("v", "s-late"))
	require.NoError(t, idx.SnapshotCommit("v", "s-late", SnapshotMeta{
		SnapID: "s-late", CreatedAt: time.Unix(2000, 0).UTC(), BlockCount: 2,
	}))

	got, err = idx.SnapshotList("v")
	require.NoError(t, err)
	require.Len(t, got, 2)
	// Ordered by CreatedAt ascending.
	require.Equal(t, "s-early", got[0].SnapID)
	require.Equal(t, "s-late", got[1].SnapID)
	require.Equal(t, int64(1), got[0].BlockCount)
	require.Equal(t, int64(2), got[1].BlockCount)
	require.True(t, got[0].CreatedAt.Equal(time.Unix(1000, 0).UTC()))

	// SnapshotPutMeta updates meta of an existing snapshot.
	require.NoError(t, idx.SnapshotPutMeta("v", SnapshotMeta{
		SnapID: "s-late", CreatedAt: time.Unix(2000, 0).UTC(), BlockCount: 42,
	}))
	got, err = idx.SnapshotList("v")
	require.NoError(t, err)
	require.Equal(t, int64(42), got[1].BlockCount)

	// Other volume meta is isolated.
	got, err = idx.SnapshotList("other")
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestSnapshotRollbackSwapsLive(t *testing.T) {
	idx := NewBadgerIndex(newTestBadger(t)).(*badgerIndex)

	// vol "v": block 0 = canonical "k0a" initially.
	var h0a, h0b [32]byte
	h0a[0] = 1
	h0b[0] = 2
	_, err := idx.WriteBlock("v", 0, h0a, "k0a")
	require.NoError(t, err)

	// Snapshot s1 captures k0a.
	require.NoError(t, idx.SnapshotBegin("v", "s1"))
	require.NoError(t, idx.SnapshotAppendChunk("v", "s1", []SnapshotBlockEntry{{0, "k0a"}}))
	require.NoError(t, idx.SnapshotCommit("v", "s1", testMeta("s1")))

	// Overwrite block 0 with k0b — live canonical changes; snap still holds k0a.
	_, err = idx.WriteBlock("v", 0, h0b, "k0b")
	require.NoError(t, err)
	require.Equal(t, int32(1), readRefcount(t, idx, "k0a")) // only snap
	require.Equal(t, int32(1), readRefcount(t, idx, "k0b")) // only live

	// Write block 1 with k1 in live (not in snap).
	var h1 [32]byte
	h1[0] = 3
	_, err = idx.WriteBlock("v", 1, h1, "k1")
	require.NoError(t, err)

	toDelete, err := idx.SnapshotRollback("v", "s1")
	require.NoError(t, err)
	// Expected: k0b refcount → 0 (delete), k1 refcount → 0 (delete).
	// k0a refcount → 2 (snap + new live).
	require.ElementsMatch(t, []string{"k0b", "k1"}, toDelete)
	require.Equal(t, int32(2), readRefcount(t, idx, "k0a"))

	// Live block 0 now resolves to k0a.
	canon, found, err := idx.ReadBlock("v", 0)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "k0a", canon)
	// Live block 1 gone.
	_, found, err = idx.ReadBlock("v", 1)
	require.NoError(t, err)
	require.False(t, found)
}

func TestSnapshotCloneIncRefsAllSrcBlocks(t *testing.T) {
	idx := NewBadgerIndex(newTestBadger(t)).(*badgerIndex)
	var ha, hb [32]byte
	ha[0] = 1
	hb[0] = 2
	_, err := idx.WriteBlock("src", 0, ha, "ka")
	require.NoError(t, err)
	_, err = idx.WriteBlock("src", 1, hb, "kb")
	require.NoError(t, err)

	require.NoError(t, idx.SnapshotClone("src", "dst"))

	// Dst sees same canonicals.
	canon, found, err := idx.ReadBlock("dst", 0)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "ka", canon)

	// Refcounts doubled.
	require.Equal(t, int32(2), readRefcount(t, idx, "ka"))
	require.Equal(t, int32(2), readRefcount(t, idx, "kb"))
}

func TestSnapshotRecoverInProgress(t *testing.T) {
	idx := NewBadgerIndex(newTestBadger(t)).(*badgerIndex)
	var h [32]byte
	h[0] = 1
	_, err := idx.WriteBlock("v", 0, h, "k0")
	require.NoError(t, err)

	require.NoError(t, idx.SnapshotBegin("v", "begun"))
	require.NoError(t, idx.SnapshotAppendChunk("v", "begun", []SnapshotBlockEntry{{0, "k0"}}))
	// simulate crash: no Commit / no Abort.

	inProg, err := idx.SnapshotListInProgress()
	require.NoError(t, err)
	require.Len(t, inProg, 1)
	require.Equal(t, "v", inProg[0].Vol)
	require.Equal(t, "begun", inProg[0].SnapID)

	toDel, err := idx.SnapshotAbort("v", "begun")
	require.NoError(t, err)
	require.Empty(t, toDel)
	require.Equal(t, int32(1), readRefcount(t, idx, "k0"))
	// state entry gone
	inProg, err = idx.SnapshotListInProgress()
	require.NoError(t, err)
	require.Empty(t, inProg)
}

func TestSnapshotListPendingRollbacksDiscoversMarkers(t *testing.T) {
	idx := NewBadgerIndex(newTestBadger(t)).(*badgerIndex)

	// No markers yet.
	got, err := idx.SnapshotListPendingRollbacks()
	require.NoError(t, err)
	require.Empty(t, got)

	// Inject stuck rollback markers (simulate crash mid-Rollback).
	require.NoError(t, idx.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(rollbackStateKey("v1", "s1"), []byte{1}); err != nil {
			return err
		}
		return txn.Set(rollbackStateKey("v2", "s7"), []byte{1})
	}))

	got, err = idx.SnapshotListPendingRollbacks()
	require.NoError(t, err)
	require.ElementsMatch(t, []struct{ Vol, SnapID string }{
		{"v1", "s1"}, {"v2", "s7"},
	}, got)

	// After SnapshotRollback completes for v1/s1, marker is gone.
	// (We can't run real Rollback here without a committed snapshot;
	// simulate by deleting the marker.)
	require.NoError(t, idx.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(rollbackStateKey("v1", "s1"))
	}))

	got, err = idx.SnapshotListPendingRollbacks()
	require.NoError(t, err)
	require.Equal(t, []struct{ Vol, SnapID string }{{"v2", "s7"}}, got)
}

func TestSnapshotListPendingClonesDiscoversMarkers(t *testing.T) {
	idx := NewBadgerIndex(newTestBadger(t)).(*badgerIndex)

	got, err := idx.SnapshotListPendingClones()
	require.NoError(t, err)
	require.Empty(t, got)

	require.NoError(t, idx.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(cloneStateKey("dst1"), []byte{1}); err != nil {
			return err
		}
		return txn.Set(cloneStateKey("dst2"), []byte{1})
	}))

	got, err = idx.SnapshotListPendingClones()
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"dst1", "dst2"}, got)

	// After a successful SnapshotClone, the marker is cleared.
	// Verify by running real Clone (which clears its own marker on success).
	var h [32]byte
	h[0] = 1
	_, err = idx.WriteBlock("src", 0, h, "k0")
	require.NoError(t, err)
	require.NoError(t, idx.SnapshotClone("src", "fresh-dst"))

	// fresh-dst should NOT appear as pending.
	got, err = idx.SnapshotListPendingClones()
	require.NoError(t, err)
	require.NotContains(t, got, "fresh-dst")
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
