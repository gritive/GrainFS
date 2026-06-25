package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// TestHeadObjectMetaBlobAuthOn covers the blob-authoritative branch at the top of
// headObjectMeta: the per-version blob tree is the BLOB AUTHORITY for vid-bearing
// versioned objects, with carve-out classes (appendable/coalesced/legacy bare)
// falling back to the FSM record. Blob absence for a versioned object = 404.
func TestHeadObjectMetaBlobAuthOn(t *testing.T) {
	ctx := context.Background()

	t.Run("live per-version blob → returns blob object", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "bon"))
		setVersioningForTest(t, b, "bon", "Enabled")
		seedVersionBlob(t, b, "bon", "k", "v1", PutObjectMetaCmd{ETag: "etag-blob", Size: 42})

		obj, _, err := b.headObjectMeta(ctx, "bon", "k")
		require.NoError(t, err)
		require.NotNil(t, obj)
		require.Equal(t, "etag-blob", obj.ETag)
		require.Equal(t, int64(42), obj.Size)
		require.Equal(t, "v1", obj.VersionID)
	})

	t.Run("blob hard-deleted but stale versioned FSM record lingers → 404", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "bstale"))
		setVersioningForTest(t, b, "bstale", "Enabled")
		// No per-version blob. A stale vid-bearing, non-appendable FSM record lingers.
		seedFSMObject(t, b, "bstale", "k", "v1", objectMeta{Key: "k", ETag: "stale"}, true)

		obj, _, err := b.headObjectMeta(ctx, "bstale", "k")
		require.ErrorIs(t, err, storage.ErrObjectNotFound)
		require.Nil(t, obj, "stale versioned FSM record must NOT be resurrected under blob-authoritative")
	})

	t.Run("delete-marker-only per-version blob → 404 (no fallthrough)", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "bdm"))
		setVersioningForTest(t, b, "bdm", "Enabled")
		seedVersionBlob(t, b, "bdm", "k", "v1", PutObjectMetaCmd{ETag: deleteMarkerETag, IsDeleteMarker: true})
		// A stale FSM record also lingers — must NOT be used; derive found not-live.
		seedFSMObject(t, b, "bdm", "k", "v1", objectMeta{Key: "k", ETag: "stale"}, true)

		obj, _, err := b.headObjectMeta(ctx, "bdm", "k")
		require.ErrorIs(t, err, storage.ErrObjectNotFound)
		require.Nil(t, obj)
	})

	t.Run("appendable carve-out (blob) → returns blob object", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "bapp"))
		setVersioningForTest(t, b, "bapp", "Enabled")
		seedVersionBlob(t, b, "bapp", "k", "v1", PutObjectMetaCmd{
			ETag: "app", IsAppendable: true, NodeIDs: []string{b.currentSelfAddr()},
		})

		obj, _, err := b.headObjectMeta(ctx, "bapp", "k")
		require.NoError(t, err)
		require.NotNil(t, obj)
		require.True(t, obj.IsAppendable)
		require.Equal(t, "app", obj.ETag)
	})

	t.Run("coalesced carve-out (blob) → returns blob object", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "bco"))
		setVersioningForTest(t, b, "bco", "Enabled")
		seedVersionBlob(t, b, "bco", "k", "v1", PutObjectMetaCmd{
			ETag: "co", IsAppendable: true,
			Coalesced: []CoalescedShardRef{{CoalescedID: "c1", Size: 10}},
			NodeIDs:   []string{b.currentSelfAddr()},
		})

		obj, _, err := b.headObjectMeta(ctx, "bco", "k")
		require.NoError(t, err)
		require.NotNil(t, obj)
		require.Len(t, obj.Coalesced, 1)
	})

	t.Run("legacy bare-unversioned record (latest-only blob) → returns blob object", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "bleg"))
		// Non-versioned legacy object: latest-only quorum-meta blob is the authority.
		seedLatestBlob(t, b, "bleg", "k", PutObjectMetaCmd{
			ETag: "legacy", NodeIDs: []string{b.currentSelfAddr()},
		})

		obj, _, err := b.headObjectMeta(ctx, "bleg", "k")
		require.NoError(t, err)
		require.NotNil(t, obj)
		require.Equal(t, "legacy", obj.ETag)
		require.Equal(t, "", obj.VersionID, "bare-legacy carries no version identity")
	})

	t.Run("blob-authority read errors → error propagated (fail closed)", func(t *testing.T) {
		b, db := newTestDistributedBackendWithDB(t)
		require.NoError(t, b.CreateBucket(ctx, "berr"))
		require.NoError(t, db.Close())

		obj, _, err := b.headObjectMeta(ctx, "berr", "k")
		require.Error(t, err)
		require.Nil(t, obj)
	})
}

// TestHeadObjectMetaBlobAuthOffUnchanged confirms the not-on (off/pending) path
// is byte-identical to today's availability-first behavior: both a per-version
// blob hit AND an FSM-fallback hit still resolve.
func TestHeadObjectMetaBlobAuthOffUnchanged(t *testing.T) {
	ctx := context.Background()

	t.Run("off: per-version blob hit resolves (availability-first)", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "voff"))
		setVersioningForTest(t, b, "voff", "Enabled")
		seedVersionBlob(t, b, "voff", "k", "v1", PutObjectMetaCmd{ETag: "etag-off", Size: 7})
		// blob-authority never flipped → off

		obj, _, err := b.headObjectMeta(ctx, "voff", "k")
		require.NoError(t, err)
		require.NotNil(t, obj)
		require.Equal(t, "etag-off", obj.ETag)
		require.Equal(t, "v1", obj.VersionID)
	})

	t.Run("off: latest-only blob hit resolves (non-versioned authority)", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "foff"))
		// Non-versioned (blob-authority off) object: the latest-only quorum-meta
		// blob is the sole authority and must resolve.
		seedLatestBlob(t, b, "foff", "k", PutObjectMetaCmd{
			ETag: "blob-off", NodeIDs: []string{b.currentSelfAddr()},
		})

		obj, _, err := b.headObjectMeta(ctx, "foff", "k")
		require.NoError(t, err)
		require.NotNil(t, obj)
		require.Equal(t, "blob-off", obj.ETag)
	})

	t.Run("blob-absent plain-versioned FSM record is 404 (blob-primary, no FSM resurrection)", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "pend"))
		setVersioningForTest(t, b, "pend", "Enabled")
		// A vid-bearing FSM record with NO per-version blob. Under blob-primary the
		// blob is the BLOB authority for a plain versioned object, so a stale FSM
		// record must NOT resurrect it — 404. (Was the old availability-first resolve.)
		seedFSMObject(t, b, "pend", "k", "v1", objectMeta{Key: "k", ETag: "pend-fsm"}, true)

		_, _, err := b.headObjectMeta(ctx, "pend", "k")
		require.ErrorIs(t, err, storage.ErrObjectNotFound)
	})
}
