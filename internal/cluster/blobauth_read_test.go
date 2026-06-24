package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// seedFSMObject writes a raw FSM obj: record (versioned when versionID != "",
// bare otherwise) plus an optional lat: pointer, mirroring how headObjectMeta's
// FSM fallback resolves records.
func seedFSMObject(t *testing.T, b *DistributedBackend, bucket, key, versionID string, m objectMeta, withLatest bool) {
	t.Helper()
	raw, err := marshalObjectMeta(m)
	require.NoError(t, err)
	require.NoError(t, b.store.Update(func(txn MetadataTxn) error {
		metaKey := b.ks().ObjectMetaKey(bucket, key)
		if versionID != "" {
			metaKey = b.ks().ObjectMetaKeyV(bucket, key, versionID)
		}
		if err := txn.Set(metaKey, raw); err != nil {
			return err
		}
		if withLatest {
			if err := txn.Set(b.ks().LatestKey(bucket, key), []byte(versionID)); err != nil {
				return err
			}
		}
		return nil
	}))
}

// TestBlobAuthReadOn covers the on→true / off,pending→false mapping and the
// fail-closed error contract.
func TestBlobAuthReadOn(t *testing.T) {
	ctx := context.Background()

	t.Run("versioning-enabled bucket is blob-authoritative", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "bon"))
		setVersioningForTest(t, b, "bon", "Enabled")
		on, err := b.blobAuthReadOn("bon")
		require.NoError(t, err)
		require.True(t, on)
	})

	t.Run("suspended bucket is not blob-authoritative", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "bsus"))
		setVersioningForTest(t, b, "bsus", "Suspended")
		on, err := b.blobAuthReadOn("bsus")
		require.NoError(t, err)
		require.False(t, on)
	})

	t.Run("unversioned bucket is not blob-authoritative", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "boff"))
		// never enabled → unversioned
		on, err := b.blobAuthReadOn("boff")
		require.NoError(t, err)
		require.False(t, on)
	})

	t.Run("fails closed when MetaBucketStore is unwired", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		b.SetMetaBucketStore(nil)
		on, err := b.blobAuthReadOn("berr")
		require.Error(t, err)
		require.False(t, on, "must fail closed (false) on a versioning-read error")
	})
}

// TestFSMCarveoutObject covers the carve-out classification matrix: the bare-vs-
// versioned decision is resolved from the read KEY, not the versionID argument.
func TestFSMCarveoutObject(t *testing.T) {
	ctx := context.Background()

	t.Run("appendable latest → carve-out", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "ba"))
		seedFSMObject(t, b, "ba", "k", "v1", objectMeta{Key: "k", ETag: "e", IsAppendable: true}, true)
		obj, _, ok, err := b.fsmCarveoutObject("ba", "k", "")
		require.NoError(t, err)
		require.True(t, ok)
		require.NotNil(t, obj)
		require.True(t, obj.IsAppendable)
		require.Equal(t, "v1", obj.VersionID, "latest read carries the resolved vid")
	})

	t.Run("coalesced latest → carve-out", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "bc"))
		m := objectMeta{Key: "k", ETag: "e", Coalesced: []CoalescedShardRef{{CoalescedID: "c1", Size: 10}}}
		seedFSMObject(t, b, "bc", "k", "v1", m, true)
		obj, _, ok, err := b.fsmCarveoutObject("bc", "k", "")
		require.NoError(t, err)
		require.True(t, ok)
		require.NotNil(t, obj)
		require.Len(t, obj.Coalesced, 1)
	})

	t.Run("legacy bare unversioned (no lat:) → carve-out", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "bl"))
		// bare obj: record, NO lat: pointer, NOT appendable/coalesced.
		seedFSMObject(t, b, "bl", "k", "", objectMeta{Key: "k", ETag: "e"}, false)
		obj, pm, ok, err := b.fsmCarveoutObject("bl", "k", "")
		require.NoError(t, err)
		require.True(t, ok, "bare-legacy unversioned record is a carve-out")
		require.NotNil(t, obj)
		require.Equal(t, "", obj.VersionID, "bare-legacy has no version identity")
		require.Equal(t, "e", obj.ETag)
		require.Equal(t, "", pm.VersionID)
	})

	t.Run("plain versioned latest (lat: present, not appendable) → NOT carve-out", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "bv"))
		seedFSMObject(t, b, "bv", "k", "v1", objectMeta{Key: "k", ETag: "e"}, true)
		obj, _, ok, err := b.fsmCarveoutObject("bv", "k", "")
		require.NoError(t, err)
		require.False(t, ok, "a plain versioned record must NOT be a carve-out under blob-authoritative")
		require.Nil(t, obj)
	})

	t.Run("specific-version plain versioned → NOT carve-out (never bare-legacy)", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "bsv"))
		// vid-bearing record, no lat: needed for a specific-version read.
		seedFSMObject(t, b, "bsv", "k", "v1", objectMeta{Key: "k", ETag: "e"}, false)
		obj, _, ok, err := b.fsmCarveoutObject("bsv", "k", "v1")
		require.NoError(t, err)
		require.False(t, ok)
		require.Nil(t, obj)
	})

	t.Run("specific-version appendable → carve-out", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "bsa"))
		seedFSMObject(t, b, "bsa", "k", "v1", objectMeta{Key: "k", ETag: "e", IsAppendable: true}, false)
		obj, _, ok, err := b.fsmCarveoutObject("bsa", "k", "v1")
		require.NoError(t, err)
		require.True(t, ok)
		require.NotNil(t, obj)
		require.Equal(t, "v1", obj.VersionID)
	})

	t.Run("absent record → false, no error", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "bn"))
		obj, pm, ok, err := b.fsmCarveoutObject("bn", "missing", "")
		require.NoError(t, err)
		require.False(t, ok)
		require.Nil(t, obj)
		require.Equal(t, PlacementMeta{}, pm)
	})

	t.Run("store error propagates", func(t *testing.T) {
		b, db := newTestDistributedBackendWithDB(t)
		require.NoError(t, b.CreateBucket(ctx, "be"))
		require.NoError(t, db.Close())
		obj, _, ok, err := b.fsmCarveoutObject("be", "k", "")
		require.Error(t, err)
		require.False(t, ok)
		require.Nil(t, obj)
	})
}

// compile-time assertion the helper returns *storage.Object (keeps the import
// meaningful even if the test body changes).
var _ = (*storage.Object)(nil)
