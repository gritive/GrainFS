package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// TestHeadObjectMetaVSoleAuthOn covers the soleauth=on early-return at the top
// of headObjectMetaV: the per-version blob is the SOLE AUTHORITY for the exact
// requested versionID of a vid-bearing versioned object. A blob MISS never
// falls through to a stale vid-bearing FSM record — only carve-out classes
// (appendable/coalesced) resolve from the FSM. Blob absence = 404.
func TestHeadObjectMetaVSoleAuthOn(t *testing.T) {
	ctx := context.Background()

	t.Run("exact-version blob present → returns blob object", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "bon"))
		setVersioningForTest(t, b, "bon", "Enabled")
		seedVersionBlob(t, b, "bon", "k", "v1", PutObjectMetaCmd{ETag: "etag-blob", Size: 42})
		setSoleAuthForTest(t, b, "bon", soleAuthOn)

		obj, _, err := b.headObjectMetaV(ctx, "bon", "k", "v1")
		require.NoError(t, err)
		require.NotNil(t, obj)
		require.Equal(t, "etag-blob", obj.ETag)
		require.Equal(t, int64(42), obj.Size)
		require.Equal(t, "v1", obj.VersionID)
	})

	t.Run("version absent but stale vid-bearing FSM record lingers → 404", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "bstale"))
		setVersioningForTest(t, b, "bstale", "Enabled")
		// No per-version blob for v1. A stale vid-bearing, non-carve-out FSM record lingers.
		seedFSMObject(t, b, "bstale", "k", "v1", objectMeta{Key: "k", ETag: "stale"}, true)
		setSoleAuthForTest(t, b, "bstale", soleAuthOn)

		obj, _, err := b.headObjectMetaV(ctx, "bstale", "k", "v1")
		require.ErrorIs(t, err, storage.ErrObjectNotFound)
		require.Nil(t, obj, "stale versioned FSM record must NOT be resurrected under soleauth=on")
	})

	t.Run("delete-marker version blob → ErrMethodNotAllowed", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "bdm"))
		setVersioningForTest(t, b, "bdm", "Enabled")
		seedVersionBlob(t, b, "bdm", "k", "v1", PutObjectMetaCmd{ETag: deleteMarkerETag, IsDeleteMarker: true})
		setSoleAuthForTest(t, b, "bdm", soleAuthOn)

		obj, _, err := b.headObjectMetaV(ctx, "bdm", "k", "v1")
		require.ErrorIs(t, err, storage.ErrMethodNotAllowed)
		require.Nil(t, obj)
	})

	t.Run("appendable carve-out (no blob) → returns FSM object", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "bapp"))
		setVersioningForTest(t, b, "bapp", "Enabled")
		seedFSMObject(t, b, "bapp", "k", "v1", objectMeta{Key: "k", ETag: "app", IsAppendable: true}, true)
		setSoleAuthForTest(t, b, "bapp", soleAuthOn)

		obj, _, err := b.headObjectMetaV(ctx, "bapp", "k", "v1")
		require.NoError(t, err)
		require.NotNil(t, obj)
		require.True(t, obj.IsAppendable)
		require.Equal(t, "app", obj.ETag)
	})

	t.Run("coalesced carve-out (no blob) → returns FSM object", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "bco"))
		setVersioningForTest(t, b, "bco", "Enabled")
		m := objectMeta{Key: "k", ETag: "co", Coalesced: []CoalescedShardRef{{CoalescedID: "c1", Size: 10}}}
		seedFSMObject(t, b, "bco", "k", "v1", m, true)
		setSoleAuthForTest(t, b, "bco", soleAuthOn)

		obj, _, err := b.headObjectMetaV(ctx, "bco", "k", "v1")
		require.NoError(t, err)
		require.NotNil(t, obj)
		require.Len(t, obj.Coalesced, 1)
	})

	t.Run("soleauth read errors → error propagated (fail closed)", func(t *testing.T) {
		b, db := newTestDistributedBackendWithDB(t)
		require.NoError(t, b.CreateBucket(ctx, "berr"))
		require.NoError(t, db.Close())

		obj, _, err := b.headObjectMetaV(ctx, "berr", "k", "v1")
		require.Error(t, err)
		require.Nil(t, obj)
	})
}

// TestHeadObjectMetaVSoleAuthOffUnchanged confirms the not-on (off/pending) path
// is byte-identical to today's specific-version behavior: both a per-version
// blob hit AND an FSM-fallback hit still resolve.
func TestHeadObjectMetaVSoleAuthOffUnchanged(t *testing.T) {
	ctx := context.Background()

	t.Run("off: per-version blob hit resolves", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "voff"))
		setVersioningForTest(t, b, "voff", "Enabled")
		seedVersionBlob(t, b, "voff", "k", "v1", PutObjectMetaCmd{ETag: "etag-off", Size: 7})
		// soleauth never flipped → off

		obj, _, err := b.headObjectMetaV(ctx, "voff", "k", "v1")
		require.NoError(t, err)
		require.NotNil(t, obj)
		require.Equal(t, "etag-off", obj.ETag)
		require.Equal(t, "v1", obj.VersionID)
	})

	t.Run("blob-absent plain-versioned FSM record is 404 (blob-primary, no FSM resurrection)", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "foff"))
		setVersioningForTest(t, b, "foff", "Enabled")
		// A vid-bearing FSM record with NO per-version blob. Under blob-primary the
		// per-version blob is the sole authority, so a specific-version read of a
		// blob-absent plain versioned record is 404 (was the old FSM ObjectMetaKeyV read).
		seedFSMObject(t, b, "foff", "k", "v1", objectMeta{Key: "k", ETag: "fsm-off"}, true)

		_, _, err := b.headObjectMetaV(ctx, "foff", "k", "v1")
		require.ErrorIs(t, err, storage.ErrObjectNotFound)
	})
}
