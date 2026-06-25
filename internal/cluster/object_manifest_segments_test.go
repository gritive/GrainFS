package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// seedNonVersionedObjectWithSegments writes a latest-only quorum-meta blob (the
// non-versioned object authority) carrying the given Segments (and optionally
// Coalesced) onto b's local ShardService.
func seedNonVersionedObjectWithSegments(
	t *testing.T,
	b *DistributedBackend,
	bucket, key string,
	segs []SegmentMetaEntry,
	coalesced []CoalescedShardRef,
) {
	t.Helper()
	seedLatestBlob(t, b, bucket, key, PutObjectMetaCmd{
		ETag: "etag-seg", NodeIDs: []string{b.currentSelfAddr()},
		Segments: segs, Coalesced: coalesced,
	})
}

func TestListAllObjectsStrictPropagatesSegments(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "bkt"))

	segs := []SegmentMetaEntry{
		{BlobID: "chunk-A", Size: 10, SegmentIdx: 0},
		{BlobID: "chunk-B", Size: 20, SegmentIdx: 1},
	}
	seedNonVersionedObjectWithSegments(t, b, "bkt", "k", segs, nil)

	objs, err := b.ListAllObjectsStrict()
	require.NoError(t, err)

	var found *storage.SnapshotObject
	for i := range objs {
		if objs[i].Key == "k" {
			found = &objs[i]
			break
		}
	}
	require.NotNil(t, found, "object k not found in ListAllObjectsStrict")
	require.Len(t, found.Segments, 2, "Segments propagation gap: want 2, got %d", len(found.Segments))
	require.Equal(t, "chunk-A", found.Segments[0].BlobID)
	require.Equal(t, "chunk-B", found.Segments[1].BlobID)
}

func TestListAllObjectsStrictPropagatesCoalesced(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "bkt2"))

	segs := []SegmentMetaEntry{
		{BlobID: "chunk-A", Size: 10, SegmentIdx: 0},
		{BlobID: "chunk-B", Size: 20, SegmentIdx: 1},
	}
	coal := []CoalescedShardRef{
		{CoalescedID: "coalesced-blob-1"},
	}
	seedNonVersionedObjectWithSegments(t, b, "bkt2", "obj", segs, coal)

	objs, err := b.ListAllObjectsStrict()
	require.NoError(t, err)

	var found *storage.SnapshotObject
	for i := range objs {
		if objs[i].Key == "obj" {
			found = &objs[i]
			break
		}
	}
	require.NotNil(t, found, "object obj not found in ListAllObjectsStrict")
	require.Len(t, found.Segments, 2)
	require.Len(t, found.Coalesced, 1, "Coalesced propagation gap")
	require.Equal(t, "coalesced-blob-1", found.Coalesced[0].CoalescedID)
}

// TestListAllObjectsStrictFailsClosedOnCorruptMeta proves the GC known-set
// contract: an undecodable latest-only quorum-meta blob makes ListAllObjectsStrict
// fail closed (returns an error) so the scrubber skips its sweep rather than
// treating the corrupt object's segments as orphaned.
func TestListAllObjectsStrictFailsClosedOnCorruptMeta(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "bkt"))

	// One valid object alongside the corrupt one.
	seedNonVersionedObjectWithSegments(t, b, "bkt", "good",
		[]SegmentMetaEntry{{BlobID: "chunk-A", Size: 10, SegmentIdx: 0}}, nil)

	// Inject an undecodable latest-only quorum-meta blob.
	require.NoError(t, b.shardSvc.writeQuorumMetaLocal("bkt", "corrupt", []byte{0xff, 0xff, 0xff}))

	_, err := b.ListAllObjectsStrict()
	require.Error(t, err, "strict ListAllObjectsStrict must fail closed on corrupt meta")
	require.Contains(t, err.Error(), "gc known-set")
}

// TestListAllObjectsStrict_BlobAuthOn_EnumeratesPerVersionBlobs proves the
// segment-GC known-set covers a blob-authoritative bucket's per-version quorum-meta
// blobs (the listBlobAuthBucketObjectsForGC branch). Without this, the sweep
// would treat an on-bucket's live segments as orphaned and delete them.
func TestListAllObjectsStrict_BlobAuthOn_EnumeratesPerVersionBlobs(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "sa"))
	setVersioningForTest(t, b, "sa", "Enabled")

	// Seed two per-version blobs while still epoch-0 (before the blob-authority flip
	// bumps the fence epoch and would reject an epoch-0 local write).
	seedVersionBlob(t, b, "sa", "k", "v1", PutObjectMetaCmd{ETag: "e1", Size: 10})
	seedVersionBlob(t, b, "sa", "k", "v2", PutObjectMetaCmd{ETag: "e2", Size: 20})

	objs, err := b.ListAllObjectsStrict()
	require.NoError(t, err)

	var saObjs []storage.SnapshotObject
	for _, o := range objs {
		if o.Bucket == "sa" {
			saObjs = append(saObjs, o)
		}
	}
	require.Len(t, saObjs, 2, "both per-version blobs must be in the GC known-set")

	v1 := byVID(t, saObjs, "v1")
	v2 := byVID(t, saObjs, "v2")
	require.Equal(t, "k", v1.Key)
	require.Equal(t, "e1", v1.ETag)
	require.False(t, v1.IsLatest, "v1 is not the max-VID version")
	require.Equal(t, "e2", v2.ETag)
	require.True(t, v2.IsLatest, "v2 is the max-VID (latest) version")
}

// TestListAllObjectsStrict_VersioningEnabled_EnumeratesPerVersionBlobs proves the
// C1 gate flip: the segment-GC known-set must cover a versioning-Enabled bucket's
// per-version blobs WITHOUT requiring the (now-vestigial) blob-authoritative flip. Under
// blob-primary the per-version blob is authoritative for every versioning-Enabled
// bucket, so listAllObjectsForGC must take its blob branch on versioning state, not
// on GetBucketBlobAuthority==on. Without the flip these blob-only objects fall to the
// FSM obj: scan, are absent, and the sweep would orphan-delete their live segments.
func TestListAllObjectsStrict_VersioningEnabled_EnumeratesPerVersionBlobs(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "ve"))
	setVersioningForTest(t, b, "ve", "Enabled")

	// Blob-only object (no FSM obj: record), blob-authority left OFF.
	seedVersionBlob(t, b, "ve", "k", "v1", PutObjectMetaCmd{ETag: "e1", Size: 10})
	seedVersionBlob(t, b, "ve", "k", "v2", PutObjectMetaCmd{ETag: "e2", Size: 20})

	objs, err := b.ListAllObjectsStrict()
	require.NoError(t, err)

	var veObjs []storage.SnapshotObject
	for _, o := range objs {
		if o.Bucket == "ve" {
			veObjs = append(veObjs, o)
		}
	}
	require.Len(t, veObjs, 2, "both per-version blobs must be in the GC known-set on versioning state alone")

	v2 := byVID(t, veObjs, "v2")
	require.Equal(t, "e2", v2.ETag)
	require.True(t, v2.IsLatest, "v2 is the max-VID (latest) version")
}
