package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// TestHeadObjectMetaSoleAuthOn covers the soleauth=on branch at the top of
// headObjectMeta: the per-version blob tree is the SOLE AUTHORITY for vid-bearing
// versioned objects, with carve-out classes (appendable/coalesced/legacy bare)
// falling back to the FSM record. Blob absence for a versioned object = 404.
func TestHeadObjectMetaSoleAuthOn(t *testing.T) {
	ctx := context.Background()

	t.Run("live per-version blob → returns blob object", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "bon"))
		setVersioningForTest(t, b, "bon", "Enabled")
		seedVersionBlob(t, b, "bon", "k", "v1", PutObjectMetaCmd{ETag: "etag-blob", Size: 42})
		setSoleAuthForTest(t, b, "bon", soleAuthOn)

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
		setSoleAuthForTest(t, b, "bstale", soleAuthOn)

		obj, _, err := b.headObjectMeta(ctx, "bstale", "k")
		require.ErrorIs(t, err, storage.ErrObjectNotFound)
		require.Nil(t, obj, "stale versioned FSM record must NOT be resurrected under soleauth=on")
	})

	t.Run("delete-marker-only per-version blob → 404 (no fallthrough)", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "bdm"))
		setVersioningForTest(t, b, "bdm", "Enabled")
		seedVersionBlob(t, b, "bdm", "k", "v1", PutObjectMetaCmd{ETag: deleteMarkerETag, IsDeleteMarker: true})
		// A stale FSM record also lingers — must NOT be used; derive found not-live.
		seedFSMObject(t, b, "bdm", "k", "v1", objectMeta{Key: "k", ETag: "stale"}, true)
		setSoleAuthForTest(t, b, "bdm", soleAuthOn)

		obj, _, err := b.headObjectMeta(ctx, "bdm", "k")
		require.ErrorIs(t, err, storage.ErrObjectNotFound)
		require.Nil(t, obj)
	})

	t.Run("appendable carve-out (no blob) → returns FSM object", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "bapp"))
		setVersioningForTest(t, b, "bapp", "Enabled")
		seedFSMObject(t, b, "bapp", "k", "v1", objectMeta{Key: "k", ETag: "app", IsAppendable: true}, true)
		setSoleAuthForTest(t, b, "bapp", soleAuthOn)

		obj, _, err := b.headObjectMeta(ctx, "bapp", "k")
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

		obj, _, err := b.headObjectMeta(ctx, "bco", "k")
		require.NoError(t, err)
		require.NotNil(t, obj)
		require.Len(t, obj.Coalesced, 1)
	})

	t.Run("legacy bare-unversioned record (no lat:, no blob) → returns FSM object", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "bleg"))
		// Bucket need not be versioning-enabled for a legacy carve-out; flip soleauth on.
		seedFSMObject(t, b, "bleg", "k", "", objectMeta{Key: "k", ETag: "legacy"}, false)
		setSoleAuthForTest(t, b, "bleg", soleAuthOn)

		obj, _, err := b.headObjectMeta(ctx, "bleg", "k")
		require.NoError(t, err)
		require.NotNil(t, obj)
		require.Equal(t, "legacy", obj.ETag)
		require.Equal(t, "", obj.VersionID, "bare-legacy carries no version identity")
	})

	t.Run("soleauth read errors → error propagated (fail closed)", func(t *testing.T) {
		b, db := newTestDistributedBackendWithDB(t)
		require.NoError(t, b.CreateBucket(ctx, "berr"))
		require.NoError(t, db.Close())

		obj, _, err := b.headObjectMeta(ctx, "berr", "k")
		require.Error(t, err)
		require.Nil(t, obj)
	})
}

// TestHeadObjectMetaSoleAuthOffUnchanged confirms the not-on (off/pending) path
// is byte-identical to today's availability-first behavior: both a per-version
// blob hit AND an FSM-fallback hit still resolve.
func TestHeadObjectMetaSoleAuthOffUnchanged(t *testing.T) {
	ctx := context.Background()

	t.Run("off: per-version blob hit resolves (availability-first)", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "voff"))
		setVersioningForTest(t, b, "voff", "Enabled")
		seedVersionBlob(t, b, "voff", "k", "v1", PutObjectMetaCmd{ETag: "etag-off", Size: 7})
		// soleauth never flipped → off

		obj, _, err := b.headObjectMeta(ctx, "voff", "k")
		require.NoError(t, err)
		require.NotNil(t, obj)
		require.Equal(t, "etag-off", obj.ETag)
		require.Equal(t, "v1", obj.VersionID)
	})

	t.Run("off: FSM-fallback hit resolves (no blob, plain versioned FSM record)", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "foff"))
		// Internal/legacy path: a bare obj: FSM record, no blob, soleauth off.
		// Under off this must still resolve via the availability-first FSM fallback.
		seedFSMObject(t, b, "foff", "k", "", objectMeta{Key: "k", ETag: "fsm-off"}, false)

		obj, _, err := b.headObjectMeta(ctx, "foff", "k")
		require.NoError(t, err)
		require.NotNil(t, obj)
		require.Equal(t, "fsm-off", obj.ETag)
	})

	t.Run("pending: stale versioned FSM record still resolves (availability-first, NOT 404)", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "pend"))
		setVersioningForTest(t, b, "pend", "Enabled")
		// No blob; a vid-bearing FSM record. Under pending the availability-first
		// fallback resolves it (this is exactly the behavior soleauth=on removes).
		seedFSMObject(t, b, "pend", "k", "v1", objectMeta{Key: "k", ETag: "pend-fsm"}, true)
		setSoleAuthForTest(t, b, "pend", soleAuthPending)

		obj, _, err := b.headObjectMeta(ctx, "pend", "k")
		require.NoError(t, err)
		require.NotNil(t, obj)
		require.Equal(t, "pend-fsm", obj.ETag)
	})
}
