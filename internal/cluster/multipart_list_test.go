package cluster

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// completeMultipartObject is a test helper: create → upload one part → complete.
func completeMultipartObject(t *testing.T, b *DistributedBackend, bucket, key string, body []byte) {
	t.Helper()
	ctx := context.Background()
	up, err := b.CreateMultipartUpload(ctx, bucket, key, "application/octet-stream")
	require.NoError(t, err)
	part, err := b.UploadPart(ctx, bucket, key, up.UploadID, 1, bytes.NewReader(body), "")
	require.NoError(t, err)
	_, err = b.CompleteMultipartUpload(ctx, bucket, key, up.UploadID, []storage.Part{*part})
	require.NoError(t, err)
}

// TestCompleteMultipart_ObjectAppearsInList is the regression guard for the
// multipart-LIST bug: a completed multipart object is written to quorum-meta
// (the Phase 4 index-free LIST source) so ListObjects enumerates it, matching
// a regular PUT. Before the fix the object's meta lived only on group-raft FSM
// and LIST (which scans quorum-meta) omitted it.
func TestCompleteMultipart_ObjectAppearsInList(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "bucket"))

	completeMultipartObject(t, b, "bucket", "mp.bin", bytes.Repeat([]byte("x"), 1<<20))

	objs, err := b.ListObjects(ctx, "bucket", "", 1000)
	require.NoError(t, err)
	keys := make([]string, 0, len(objs))
	for _, o := range objs {
		keys = append(keys, o.Key)
	}
	require.Contains(t, keys, "mp.bin", "completed multipart object must be enumerable by LIST")
}

// TestCompleteMultipart_DeleteThenAbsent guards the dual-write delete
// interaction: after the object is committed to quorum-meta (and a redundant
// FSM copy), a DELETE writes a quorum-meta tombstone that must shadow it on
// both HEAD (404 short-circuit before the FSM fallback) and LIST (tombstone
// filtered) — the FSM copy must not resurface it.
func TestCompleteMultipart_DeleteThenAbsent(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "bucket"))

	completeMultipartObject(t, b, "bucket", "mp.bin", bytes.Repeat([]byte("y"), 1<<20))
	require.NoError(t, b.DeleteObject(ctx, "bucket", "mp.bin"))

	_, err := b.HeadObject(ctx, "bucket", "mp.bin")
	require.ErrorIs(t, err, storage.ErrObjectNotFound, "deleted multipart object must not resurface via FSM fallback")

	objs, err := b.ListObjects(ctx, "bucket", "", 1000)
	require.NoError(t, err)
	for _, o := range objs {
		require.NotEqual(t, "mp.bin", o.Key, "deleted multipart object must be absent from LIST")
	}
}
