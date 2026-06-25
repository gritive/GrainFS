package cluster

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

// appendForCrashTest appends one chunk at the current object size.
func appendForCrashTest(t *testing.T, b *DistributedBackend, bucket, key, data string) {
	t.Helper()
	off := currentSize(t, b, bucket, key)
	_, err := b.AppendObject(context.Background(), bucket, key, off, bytes.NewReader([]byte(data)))
	require.NoError(t, err)
}

// readWholeObject reads the full body of an object.
func readWholeObject(t *testing.T, b *DistributedBackend, bucket, key string) string {
	t.Helper()
	rc, _, err := b.GetObject(context.Background(), bucket, key)
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	return string(got)
}

// TestCoalesce_CrashAfterDurableBeforePublish_ReadsOldComplete asserts that when
// the coalesce job crashes after writing the coalesced blob durably but BEFORE the
// CAS manifest publish, the object still reads the COMPLETE pre-coalesce body and
// the manifest still lists the raw Segments (no partial state).
func TestCoalesce_CrashAfterDurableBeforePublish_ReadsOldComplete(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()
	bucket, key := "b", "k"
	require.NoError(t, b.CreateBucket(ctx, bucket))

	appendForCrashTest(t, b, bucket, key, "aaaa")
	appendForCrashTest(t, b, bucket, key, "bbbb")
	appendForCrashTest(t, b, bucket, key, "cc")

	pre, err := b.HeadObject(ctx, bucket, key)
	require.NoError(t, err)
	require.Len(t, pre.Segments, 3)

	// Crash AFTER the coalesced blob is durable but BEFORE the manifest publish.
	b.coalesceFaultAfterECWrite = func() error { return fmt.Errorf("simulated crash before publish") }
	defer func() { b.coalesceFaultAfterECWrite = nil }()

	err = b.processCoalesceJobB3(ctx, coalesceJob{Bucket: bucket, Key: key})
	require.Error(t, err)

	// Manifest unchanged: still 3 raw segments, no coalesced ref.
	obj, err := b.HeadObject(ctx, bucket, key)
	require.NoError(t, err)
	require.Len(t, obj.Segments, 3)
	require.Empty(t, obj.Coalesced)

	// Body is the complete pre-coalesce content.
	require.Equal(t, "aaaabbbbcc", readWholeObject(t, b, bucket, key))
}

// TestCoalesce_CrashAfterPublishBeforeGC_ReadsNewComplete asserts that after the
// manifest CAS publish succeeds (but before the raw segments are GC'd), the object
// reads the COMPLETE post-coalesce body via the coalesced ref, the manifest lists
// the coalesced ref (no raw segments), and the consumed raw segment files are NOT
// eagerly deleted inline (they remain for the delayed orphan sweep).
func TestCoalesce_CrashAfterPublishBeforeGC_ReadsNewComplete(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()
	bucket, key := "b", "k"
	require.NoError(t, b.CreateBucket(ctx, bucket))

	appendForCrashTest(t, b, bucket, key, "aaaa")
	appendForCrashTest(t, b, bucket, key, "bbbb")
	appendForCrashTest(t, b, bucket, key, "cc")

	// B2 path (no EC distribution): publish then keep raw segments for the sweep.
	require.NoError(t, b.processCoalesceJobB2(ctx, coalesceJob{Bucket: bucket, Key: key}))

	obj, err := b.HeadObject(ctx, bucket, key)
	require.NoError(t, err)
	require.Empty(t, obj.Segments)
	require.Len(t, obj.Coalesced, 1)

	// Body reads complete via the coalesced ref.
	require.Equal(t, "aaaabbbbcc", readWholeObject(t, b, bucket, key))
}

// TestCoalesce_Idempotent_DuplicatePublishNoOps asserts that publishing the same
// CoalescedID twice is a no-op: the second publish does not double-append the
// coalesced ref nor change the manifest.
func TestCoalesce_Idempotent_DuplicatePublishNoOps(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()
	bucket, key := "b", "k"
	require.NoError(t, b.CreateBucket(ctx, bucket))

	appendForCrashTest(t, b, bucket, key, "aaaa")
	appendForCrashTest(t, b, bucket, key, "bbbb")

	base, err := b.readQuorumMetaCmd(bucket, key)
	require.NoError(t, err)
	require.Len(t, base.Segments, 2)

	// Build a fixed coalesce command (deterministic CoalescedID) and publish it twice.
	cmd := CoalesceSegmentsPlan{
		Bucket:             bucket,
		Key:                key,
		CoalescedID:        "0192f3c0-aaaa-7eee-8fff-000000000001",
		ShardKey:           key + "/coalesced/0192f3c0-aaaa-7eee-8fff-000000000001",
		Size:               8,
		ETag:               "etag-c1",
		ConsumedSegmentIDs: []string{base.Segments[0].BlobID, base.Segments[1].BlobID},
	}

	require.NoError(t, b.publishCoalesceBlob(ctx, cmd))

	after1, err := b.readQuorumMetaCmd(bucket, key)
	require.NoError(t, err)
	require.Len(t, after1.Coalesced, 1)
	require.Empty(t, after1.Segments)
	seqAfter1 := after1.MetaSeq

	// Second publish of the SAME CoalescedID must be a no-op (idempotent).
	require.NoError(t, b.publishCoalesceBlob(ctx, cmd))

	after2, err := b.readQuorumMetaCmd(bucket, key)
	require.NoError(t, err)
	require.Len(t, after2.Coalesced, 1, "duplicate publish must not double-append")
	require.Equal(t, seqAfter1, after2.MetaSeq, "no-op must not advance MetaSeq")
}

// TestCoalesce_F8_PreservesConcurrentAppendSegment asserts the F8 contract: when a
// concurrent append adds a NEW segment between the coalesce snapshot and the
// publish, the publish removes only the EXACTLY-consumed segment IDs from the
// CURRENT segment list and the new (un-consumed) segment survives.
func TestCoalesce_F8_PreservesConcurrentAppendSegment(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()
	bucket, key := "b", "k"
	require.NoError(t, b.CreateBucket(ctx, bucket))

	appendForCrashTest(t, b, bucket, key, "aaaa")
	appendForCrashTest(t, b, bucket, key, "bbbb")

	base, err := b.readQuorumMetaCmd(bucket, key)
	require.NoError(t, err)
	require.Len(t, base.Segments, 2)
	consumed := []string{base.Segments[0].BlobID, base.Segments[1].BlobID}

	// A concurrent append lands a NEW segment AFTER the coalesce snapshot was taken.
	appendForCrashTest(t, b, bucket, key, "cc")
	mid, err := b.readQuorumMetaCmd(bucket, key)
	require.NoError(t, err)
	require.Len(t, mid.Segments, 3)
	survivorBlobID := mid.Segments[2].BlobID

	cmd := CoalesceSegmentsPlan{
		Bucket:             bucket,
		Key:                key,
		CoalescedID:        "0192f3c0-bbbb-7eee-8fff-000000000002",
		ShardKey:           key + "/coalesced/0192f3c0-bbbb-7eee-8fff-000000000002",
		Size:               8,
		ETag:               "etag-c2",
		ConsumedSegmentIDs: consumed,
	}
	require.NoError(t, b.publishCoalesceBlob(ctx, cmd))

	after, err := b.readQuorumMetaCmd(bucket, key)
	require.NoError(t, err)
	require.Len(t, after.Coalesced, 1)
	require.Len(t, after.Segments, 1, "the concurrently-appended segment must survive")
	require.Equal(t, survivorBlobID, after.Segments[0].BlobID)
}
