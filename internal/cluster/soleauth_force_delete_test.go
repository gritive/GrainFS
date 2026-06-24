package cluster

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// TestForceDeleteBucketSoleAuthOn covers the soleauth=on leaf ForceDeleteBucket
// path: the deletion set is enumerated from the per-version blob authority
// (scanQuorumMetaVersionsClusterAll + DeleteObjectVersion), and the trailing
// blob-aware DeleteBucket empties the bucket. The legacy FSM carve-out tail
// (scanFsmCarveoutVersions + HardDeleteLegacyObject) was dropped in Task 4b:
// greenfield versioned buckets have no FSM carve-outs.
func TestForceDeleteBucketSoleAuthOn(t *testing.T) {
	ctx := context.Background()

	t.Run("versioned blobs only → force-delete empties the bucket", func(t *testing.T) {
		b := newSingleNode1Plus0ChunkCapable(t)
		require.NoError(t, b.CreateBucket(ctx, "b"))
		setVersioningForTest(t, b, "b", "Enabled")
		// Versioned blobs (placement node = "self" so the purge reaches the local blob).
		seedVersionBlob(t, b, "b", "k1", vidA1, PutObjectMetaCmd{ETag: "k1v1", NodeIDs: []string{"self"}})
		seedVersionBlob(t, b, "b", "k1", vidA2, PutObjectMetaCmd{ETag: "k1v2", NodeIDs: []string{"self"}})
		seedVersionBlob(t, b, "b", "k2", vidB1, PutObjectMetaCmd{ETag: "k2v1", NodeIDs: []string{"self"}})
		// A delete-marker blob (a record that must also be removed).
		seedVersionBlob(t, b, "b", "k2", vidB2, PutObjectMetaCmd{ETag: deleteMarkerETag, IsDeleteMarker: true, NodeIDs: []string{"self"}})
		// Stale non-carve-out vid-bearing FSM record with NO blob — non-authoritative
		// under `on`: must NOT be enumerated/resurrected, and must NOT block the delete.
		seedFSMObject(t, b, "b", "ghost", vidB1, objectMeta{Key: "ghost", ETag: "stale"}, true)

		require.NoError(t, b.ForceDeleteBucket(ctx, "b"))
		require.ErrorIs(t, b.HeadBucket(ctx, "b"), storage.ErrBucketNotFound)
	})

	t.Run("soleauth read error → propagated (fail closed)", func(t *testing.T) {
		b, db := newTestDistributedBackendWithDB(t)
		require.NoError(t, b.CreateBucket(ctx, "berr"))
		require.NoError(t, db.Close())

		err := b.ForceDeleteBucket(ctx, "berr")
		require.Error(t, err)
	})
}

// TestHardDeleteLegacyObject removed: HardDeleteLegacyObject and its raft
// primitive (CmdDeleteObject = 4) are retired in data-plane raft-free Slice 2.
// Force-delete is now blob-physical; the legacy FSM-delete path is gone.

// TestForceDeleteBucketNonVersioned_QmetaAndShards confirms the non-versioned
// path uses qmeta enumerate + shards-first physical purge (not FSM-scan).
// It verifies that after ForceDeleteBucket, no qmeta blobs or shard files remain.
func TestForceDeleteBucketNonVersioned_QmetaAndShards(t *testing.T) {
	ctx := context.Background()
	b, dataDir := newSingleNodeBackendWithDirForTest(t)
	require.NoError(t, b.CreateBucket(ctx, "b"))
	// Two real objects via the full quorum-meta path.
	putTestObjectForRetire(t, b, "b", "k1", []byte("payload1"))
	putTestObjectForRetire(t, b, "b", "k2", []byte("payload2"))

	require.NoError(t, b.ForceDeleteBucket(ctx, "b"))
	require.ErrorIs(t, b.HeadBucket(ctx, "b"), storage.ErrBucketNotFound)

	// No qmeta blobs remain.
	require.NoFileExists(t, filepath.Join(dataDir, ".quorum_meta", "b"))
	// No shard files remain.
	require.Empty(t, residualShardFilesForTest(t, dataDir, "b"))
}
