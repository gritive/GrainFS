package cluster

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestNonVersionedListObjectVersions_EnumeratesBlobs proves the S3 ?versions API
// (and the lifecycle worker, which drives ScanObjectsGrouped → ListObjectVersions)
// see a non-versioned bucket's blob-only objects. A non-versioned PUT writes only
// the latest-only blob (no FSM obj: record), so the FSM-only enumeration missed
// them entirely — they were invisible to ?versions and never expired by lifecycle.
func TestNonVersionedListObjectVersions_EnumeratesBlobs(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	const bkt = "plainbkt"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	// No SetBucketVersioning → non-versioned.

	_, err := b.PutObject(ctx, bkt, "a.txt", bytes.NewReader([]byte("aaa")), "text/plain")
	require.NoError(t, err)
	_, err = b.PutObject(ctx, bkt, "b.txt", bytes.NewReader([]byte("bbbb")), "text/plain")
	require.NoError(t, err)

	vers, err := b.ListObjectVersions(ctx, bkt, "", 0)
	require.NoError(t, err)
	byKey := map[string]int{}
	for _, v := range vers {
		byKey[v.Key]++
		require.True(t, v.IsLatest, "non-versioned object is its own latest")
		require.False(t, v.IsDeleteMarker)
	}
	require.Equal(t, 1, byKey["a.txt"], "a.txt must appear exactly once in ?versions")
	require.Equal(t, 1, byKey["b.txt"], "b.txt must appear exactly once in ?versions")

	// ScanObjectsGrouped (the lifecycle enumerator) must surface both keys.
	ch, err := b.ScanObjectsGrouped(bkt)
	require.NoError(t, err)
	seen := map[string]bool{}
	for g := range ch {
		seen[g.Key] = true
		require.NotEmpty(t, g.Versions, "lifecycle group %q must carry its version", g.Key)
	}
	require.True(t, seen["a.txt"] && seen["b.txt"], "lifecycle scan must see both non-versioned objects, got %v", seen)
}

// TestNonVersionedListObjectVersions_DeletedAbsent proves a deleted (tombstoned)
// non-versioned object is absent from ?versions (the latest-only delete tombstone
// is filtered by the live-view scan).
func TestNonVersionedListObjectVersions_DeletedAbsent(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	const bkt = "plainbkt"
	require.NoError(t, b.CreateBucket(ctx, bkt))

	_, err := b.PutObject(ctx, bkt, "gone.txt", bytes.NewReader([]byte("x")), "text/plain")
	require.NoError(t, err)
	_, err = b.DeleteObjectReturningMarker(bkt, "gone.txt")
	require.NoError(t, err)

	vers, err := b.ListObjectVersions(ctx, bkt, "", 0)
	require.NoError(t, err)
	for _, v := range vers {
		require.NotEqual(t, "gone.txt", v.Key, "deleted non-versioned object must not appear in ?versions")
	}
}
