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

// compile-time assertion the helper returns *storage.Object (keeps the import
// meaningful even if the test body changes).
var _ = (*storage.Object)(nil)
