package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// seedLatestBlob writes a latest-only quorum-meta blob (.quorum_meta/{bucket}/{key})
// on b's local ShardService — the non-versioned object's authoritative metadata.
func seedLatestBlob(t *testing.T, b *DistributedBackend, bucket, key string, cmd PutObjectMetaCmd) {
	t.Helper()
	cmd.Bucket = bucket
	cmd.Key = key
	blob, err := encodeQuorumMetaBlob(cmd)
	require.NoError(t, err)
	require.NoError(t, b.shardSvc.writeQuorumMetaLocal(bucket, key, blob))
}

// TestListAllObjectsForGC_NonVersionedScansBlob proves the segment-GC known-set
// covers a NON-VERSIONED bucket's latest-only quorum-meta blobs. Under blob-primary
// a non-versioned PUT writes only the latest-only blob (no FSM obj: record), so the
// known-set must scan those blobs — not the (empty) FSM obj: tree. Without this the
// sweep treats every live non-versioned object's segments as orphaned and deletes
// them (data loss). Symmetric counterpart of listSoleAuthBucketObjectsForGC.
func TestListAllObjectsForGC_NonVersionedScansBlob(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "nv"))
	// No SetBucketVersioning → non-versioned (latest-only authority).

	// Blob-only non-versioned object carrying a segment (no FSM obj: record).
	seedLatestBlob(t, b, "nv", "k", PutObjectMetaCmd{
		ETag: "e1", Size: 100,
		NodeIDs: []string{"n1"}, ECData: 1,
		Segments: []SegmentMetaEntry{{BlobID: "seg-0", Size: 100, SegmentIdx: 0}},
	})

	objs, err := b.ListAllObjectsStrict()
	require.NoError(t, err)

	var nvObjs []storage.SnapshotObject
	for _, o := range objs {
		if o.Bucket == "nv" {
			nvObjs = append(nvObjs, o)
		}
	}
	require.Len(t, nvObjs, 1, "the non-versioned latest-only blob must be in the GC known-set")
	require.Equal(t, "k", nvObjs[0].Key)
	require.Equal(t, "e1", nvObjs[0].ETag)
	require.True(t, nvObjs[0].IsLatest, "non-versioned object is always latest")
	require.Len(t, nvObjs[0].Segments, 1, "segments must be carried so the GC protects them")
	require.Equal(t, "seg-0", nvObjs[0].Segments[0].BlobID)
}

// TestListAllObjectsForGC_NonVersionedExcludesHardDeleteTombstone proves a
// hard-delete tombstone on the latest-only key is NOT a live object in the GC
// known-set — its now-dead segments become orphan-eligible.
func TestListAllObjectsForGC_NonVersionedExcludesHardDeleteTombstone(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "nv"))

	seedLatestBlob(t, b, "nv", "tombstoned", PutObjectMetaCmd{
		ETag: "e", Size: 10, NodeIDs: []string{"n1"}, ECData: 1,
		IsHardDeleted: true,
		Segments:      []SegmentMetaEntry{{BlobID: "dead-seg", Size: 10}},
	})

	objs, err := b.ListAllObjectsStrict()
	require.NoError(t, err)
	for _, o := range objs {
		require.NotEqual(t, "tombstoned", o.Key, "hard-delete tombstone must be excluded from the GC known-set")
	}
}
