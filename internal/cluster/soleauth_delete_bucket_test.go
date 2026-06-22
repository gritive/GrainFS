package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// TestDeleteBucketEmptinessSoleAuthOn covers the soleauth=on emptiness branch of
// DistributedBackend.DeleteBucket: emptiness is derived from the per-version blob
// tree (incl. delete markers) + carve-out FSM, NOT the stale FSM obj: prefix scan.
func TestDeleteBucketEmptinessSoleAuthOn(t *testing.T) {
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
		// The core soleauth=on win: a non-carve-out vid-bearing FSM obj: record is
		// non-authoritative under sole authority, so it must NOT false-block the
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

	t.Run("soleauth read error → propagated (fail closed)", func(t *testing.T) {
		b, db := newTestDistributedBackendWithDB(t)
		require.NoError(t, b.CreateBucket(ctx, "berr"))
		require.NoError(t, db.Close())

		err := b.DeleteBucket(ctx, "berr")
		require.Error(t, err)
		require.NotErrorIs(t, err, storage.ErrBucketNotFound)
	})
}

// TestDeleteBucketEmptinessOffUnchanged confirms the off/pending path is
// byte-identical to today's FSM obj: prefix scan.
func TestDeleteBucketEmptinessOffUnchanged(t *testing.T) {
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
