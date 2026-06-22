package cluster

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
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

// TestNonVersionedListObjectVersions_MultipartNoDuplicate proves a non-versioned
// MULTIPART object appears EXACTLY ONCE in ?versions. A non-versioned multipart
// complete writes BOTH the FSM obj:/lat: records AND the latest-only blob (same
// VersionID), so without the leaf (Key,VID) dedup it would double-list (the
// single-group ?versions path and the lifecycle worker bypass the coordinator dedup).
func TestNonVersionedListObjectVersions_MultipartNoDuplicate(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	const bkt, key = "plainbkt", "mp.bin"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	// No SetBucketVersioning → non-versioned.

	up, err := b.CreateMultipartUpload(ctx, bkt, key, "application/octet-stream")
	require.NoError(t, err)
	part, err := b.UploadPart(ctx, bkt, key, up.UploadID, 1, bytes.NewReader([]byte("multipart-payload")), "")
	require.NoError(t, err)
	_, err = b.CompleteMultipartUpload(ctx, bkt, key, up.UploadID, []storage.Part{*part})
	require.NoError(t, err)

	vers, err := b.ListObjectVersions(ctx, bkt, "", 0)
	require.NoError(t, err)
	count := 0
	for _, v := range vers {
		if v.Key == key {
			count++
		}
	}
	require.Equal(t, 1, count, "non-versioned multipart object must appear exactly once in ?versions (got %d)", count)
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
