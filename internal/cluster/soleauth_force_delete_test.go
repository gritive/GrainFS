package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// TestForceDeleteBucketSoleAuthOn covers the soleauth=on leaf ForceDeleteBucket
// path: the deletion set is enumerated from the per-version blob authority +
// carve-out FSM (NOT the stale FSM obj:/lat: scan), vid-bearing entries are
// deleted with their per-version blob purged, legacy-bare carve-outs are
// hard-deleted, and the trailing blob-aware DeleteBucket empties the bucket.
func TestForceDeleteBucketSoleAuthOn(t *testing.T) {
	ctx := context.Background()

	t.Run("mixed authoritative content → force-delete empties the bucket", func(t *testing.T) {
		b := newSingleNode1Plus0ChunkCapable(t)
		require.NoError(t, b.CreateBucket(ctx, "b"))
		setVersioningForTest(t, b, "b", "Enabled")
		// Versioned blobs (placement node = "self" so the purge reaches the local blob).
		seedVersionBlob(t, b, "b", "k1", vidA1, PutObjectMetaCmd{ETag: "k1v1", NodeIDs: []string{"self"}})
		seedVersionBlob(t, b, "b", "k1", vidA2, PutObjectMetaCmd{ETag: "k1v2", NodeIDs: []string{"self"}})
		seedVersionBlob(t, b, "b", "k2", vidB1, PutObjectMetaCmd{ETag: "k2v1", NodeIDs: []string{"self"}})
		// A delete-marker blob (a record that must also be removed).
		seedVersionBlob(t, b, "b", "k2", vidB2, PutObjectMetaCmd{ETag: deleteMarkerETag, IsDeleteMarker: true, NodeIDs: []string{"self"}})
		// Appendable carve-out (vid-bearing, FSM-authoritative, no blob).
		seedFSMObject(t, b, "b", "ak", vidA1, objectMeta{Key: "ak", ETag: "app", IsAppendable: true}, true)
		// Legacy-bare carve-out (unversioned, no lat:).
		seedFSMObject(t, b, "b", "lk", "", objectMeta{Key: "lk", ETag: "bare"}, false)
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

// TestHardDeleteLegacyObject covers the new on-gated hard-delete primitive: it
// removes a legacy unversioned bare obj:{bucket}/{key} record with no tombstone.
func TestHardDeleteLegacyObject(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(ctx, "b"))
	seedFSMObject(t, b, "b", "lk", "", objectMeta{Key: "lk", ETag: "bare"}, false)

	// Present before.
	_, err := b.HeadObject(ctx, "b", "lk")
	require.NoError(t, err)

	require.NoError(t, b.HardDeleteLegacyObject(ctx, "b", "lk"))

	// Gone after (hard delete, no tombstone).
	_, err = b.HeadObject(ctx, "b", "lk")
	require.ErrorIs(t, err, storage.ErrObjectNotFound)
}

// TestForceDeleteBucketSoleAuthOffUnchanged confirms the off path still uses the
// FSM-scan + two-pass forceDeleteObject and empties the bucket.
func TestForceDeleteBucketSoleAuthOffUnchanged(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(ctx, "b"))
	setVersioningForTest(t, b, "b", "Enabled")
	seedFSMObject(t, b, "b", "k", vidA1, objectMeta{Key: "k", ETag: "v1"}, true)
	seedFSMObject(t, b, "b", "lk", "", objectMeta{Key: "lk", ETag: "bare"}, false)
	// soleauth off (default)

	require.NoError(t, b.ForceDeleteBucket(ctx, "b"))
	require.ErrorIs(t, b.HeadBucket(ctx, "b"), storage.ErrBucketNotFound)
}
