package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// TestDeleteBucketEmptinessBlobAuthOn covers the blob-authoritative emptiness branch of
// DistributedBackend.DeleteBucket: emptiness is derived from the per-version blob
// tree (incl. delete markers) + carve-out FSM, NOT the stale FSM obj: prefix scan.
func TestDeleteBucketEmptinessBlobAuthOn(t *testing.T) {
	ctx := context.Background()

	t.Run("live version present → ErrBucketNotEmpty", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "bon"))
		setVersioningForTest(t, b, "bon", "Enabled")
		seedVersionBlob(t, b, "bon", "k1", vidA1, PutObjectMetaCmd{ETag: "v1"})

		err := b.DeleteBucket(ctx, "bon")
		require.ErrorIs(t, err, storage.ErrBucketNotEmpty)
	})

	t.Run("only a delete-marker blob → ErrBucketNotEmpty (markers count)", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "bmark"))
		setVersioningForTest(t, b, "bmark", "Enabled")
		seedVersionBlob(t, b, "bmark", "k", vidA1, PutObjectMetaCmd{ETag: deleteMarkerETag, IsDeleteMarker: true})

		err := b.DeleteBucket(ctx, "bmark")
		require.ErrorIs(t, err, storage.ErrBucketNotEmpty)
	})

	t.Run("appendable carve-out → ErrBucketNotEmpty", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "bapp"))
		setVersioningForTest(t, b, "bapp", "Enabled")
		seedFSMObject(t, b, "bapp", "ak", vidA1, objectMeta{Key: "ak", ETag: "app", IsAppendable: true}, true)

		err := b.DeleteBucket(ctx, "bapp")
		require.ErrorIs(t, err, storage.ErrBucketNotEmpty)
	})

	t.Run("stale vid-bearing FSM record but NO blob → bucket EMPTY, delete proceeds", func(t *testing.T) {
		// The core blob-authoritative win: a non-carve-out vid-bearing FSM obj: record is
		// non-authoritative under blob authority, so it must NOT false-block the
		// deletion of an authoritatively-empty bucket (the off-path obj: scan would).
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "bstale"))
		setVersioningForTest(t, b, "bstale", "Enabled")
		seedFSMObject(t, b, "bstale", "ghost", vidB1, objectMeta{Key: "ghost", ETag: "stale"}, true)

		require.NoError(t, b.DeleteBucket(ctx, "bstale"))
		require.ErrorIs(t, b.HeadBucket(ctx, "bstale"), storage.ErrBucketNotFound)
	})

	t.Run("truly empty → delete succeeds", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "bempty"))
		setVersioningForTest(t, b, "bempty", "Enabled")

		require.NoError(t, b.DeleteBucket(ctx, "bempty"))
		require.ErrorIs(t, b.HeadBucket(ctx, "bempty"), storage.ErrBucketNotFound)
	})

	t.Run("blob-authority read error → propagated (fail closed)", func(t *testing.T) {
		b, db := newTestDistributedBackendWithDB(t)
		require.NoError(t, b.CreateBucket(ctx, "berr"))
		require.NoError(t, db.Close())

		err := b.DeleteBucket(ctx, "berr")
		require.Error(t, err)
		require.NotErrorIs(t, err, storage.ErrBucketNotFound)
	})
}

// TestDeleteBucketEmptinessNonEnabled covers the non-Enabled emptiness branch
// (never-versioned AND Suspended). Objects live in TWO blob trees; DeleteBucket
// must consult both or it silently deletes a non-empty bucket (data loss).
func TestDeleteBucketEmptinessNonEnabled(t *testing.T) {
	ctx := context.Background()

	t.Run("never-versioned latest-only blob present -> ErrBucketNotEmpty", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "nvb"))
		seedLatestBlob(t, b, "nvb", "k1", PutObjectMetaCmd{ETag: "e1", Size: 4, NodeIDs: []string{"n1"}})
		require.ErrorIs(t, b.DeleteBucket(ctx, "nvb"), storage.ErrBucketNotEmpty)
	})

	t.Run("suspended bucket with a preserved per-version blob -> ErrBucketNotEmpty", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "susp"))
		seedVersionBlob(t, b, "susp", "k1", vidA1, PutObjectMetaCmd{ETag: "v1"})
		setVersioningForTest(t, b, "susp", "Suspended")
		require.ErrorIs(t, b.DeleteBucket(ctx, "susp"), storage.ErrBucketNotEmpty)
	})

	t.Run("truly empty never-versioned -> delete succeeds", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "nvempty"))
		require.NoError(t, b.DeleteBucket(ctx, "nvempty"))
		require.ErrorIs(t, b.HeadBucket(ctx, "nvempty"), storage.ErrBucketNotFound)
	})

	t.Run("never-versioned with only latest-only tombstones -> empty, delete succeeds", func(t *testing.T) {
		// A non-versioned DeleteObject overwrites the latest-only blob with an
		// IsDeleteMarker tombstone (object_delete.go); IsHardDeleted is defense-in-
		// depth. Neither is a live object, so a bucket holding only tombstones must
		// still delete — the emptiness loop must skip BOTH (mirroring the LIST
		// filterAndSortEntries), or an all-deleted bucket becomes undeleteable.
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "nvtomb"))
		seedLatestBlob(t, b, "nvtomb", "del-marker", PutObjectMetaCmd{IsDeleteMarker: true, NodeIDs: []string{"n1"}})
		seedLatestBlob(t, b, "nvtomb", "hard-del", PutObjectMetaCmd{IsHardDeleted: true, NodeIDs: []string{"n1"}})
		require.NoError(t, b.DeleteBucket(ctx, "nvtomb"))
		require.ErrorIs(t, b.HeadBucket(ctx, "nvtomb"), storage.ErrBucketNotFound)
	})
}

// TestForceDeleteBucketSuspendedPurgesPerVersion proves --force purges a Suspended
// bucket's preserved per-version blobs (from a prior Enabled era), not just the
// latest-only tree, so the bucket actually deletes instead of tripping the
// per-version emptiness recheck in the final DeleteBucket.
func TestForceDeleteBucketSuspendedPurgesPerVersion(t *testing.T) {
	// Enable → write (→ real per-version blob + shards) → Suspend leaves the
	// version preserved in the per-version tree. ForceDeleteBucket on the Suspended
	// bucket must purge it (not just the empty latest-only tree).
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "suspf"))
	require.NoError(t, b.SetBucketVersioning("suspf", "Enabled"))
	putTestObjectForRetire(t, b, "suspf", "k1", []byte("a"))
	require.NoError(t, b.SetBucketVersioning("suspf", "Suspended"))

	require.NoError(t, b.ForceDeleteBucket(ctx, "suspf"))
	require.ErrorIs(t, b.HeadBucket(ctx, "suspf"), storage.ErrBucketNotFound)
}

// TestDeleteBucketEmptinessNonEnabledControl confirms blob-primary emptiness for
// the cases adjacent to the non-Enabled fix. The non-Enabled emptiness path no
// longer scans the FSM obj: tree (dead in greenfield — CmdPutObjectMeta apply is
// a no-op); it probes the latest-only + per-version blob trees instead. The
// subtests below cover (1) the Enabled (ON) branch where a stale plain-versioned
// FSM record with no per-version blob is authoritatively empty, and (2) a truly
// empty never-versioned bucket that takes the new blob-tree probe and deletes.
func TestDeleteBucketEmptinessNonEnabledControl(t *testing.T) {
	ctx := context.Background()

	t.Run("a stale plain-versioned FSM record does NOT block deletion (blob-primary)", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "voff"))
		setVersioningForTest(t, b, "voff", "Enabled")
		// A stale plain-versioned FSM record with no per-version blob is
		// non-authoritative under blob-primary, so the bucket is authoritatively
		// empty and deletes (was: the obj: scan saw the ghost and blocked).
		seedFSMObject(t, b, "voff", "ghost", vidB1, objectMeta{Key: "ghost", ETag: "stale"}, true)

		require.NoError(t, b.DeleteBucket(ctx, "voff"))
		require.ErrorIs(t, b.HeadBucket(ctx, "voff"), storage.ErrBucketNotFound)
	})

	t.Run("off: empty bucket deletes", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "voff2"))

		require.NoError(t, b.DeleteBucket(ctx, "voff2"))
		require.ErrorIs(t, b.HeadBucket(ctx, "voff2"), storage.ErrBucketNotFound)
	})
}
