package cluster

import (
	"context"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// seedObjectWithSegments writes an objectMeta carrying the given Segments
// (and optionally Coalesced) directly into the backend's BadgerDB. This
// bypasses the propose/apply path so we can seed fields (Coalesced) that
// the PutObjectMetaCmd wire format doesn't carry yet.
func seedObjectWithSegments(
	t *testing.T,
	b *DistributedBackend,
	bucket, key, versionID string,
	segs []storage.SegmentRef,
	coalesced []CoalescedShardRef,
) {
	t.Helper()
	raw, err := marshalObjectMeta(objectMeta{
		Key:       key,
		Size:      0,
		ETag:      "etag-seg",
		Segments:  segs,
		Coalesced: coalesced,
	})
	require.NoError(t, err)
	require.NoError(t, b.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(b.ks().ObjectMetaKeyV(bucket, key, versionID), raw); err != nil {
			return err
		}
		return txn.Set(b.ks().LatestKey(bucket, key), []byte(versionID))
	}))
}

func TestListAllObjectsPropagatesSegments(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "bkt"))

	segs := []storage.SegmentRef{
		{BlobID: "chunk-A", Size: 10},
		{BlobID: "chunk-B", Size: 20},
	}
	seedObjectWithSegments(t, b, "bkt", "k", "v1", segs, nil)

	objs, err := b.ListAllObjects()
	require.NoError(t, err)

	var found *storage.SnapshotObject
	for i := range objs {
		if objs[i].Key == "k" {
			found = &objs[i]
			break
		}
	}
	require.NotNil(t, found, "object k not found in ListAllObjects")
	require.Len(t, found.Segments, 2, "Segments propagation gap: want 2, got %d", len(found.Segments))
	require.Equal(t, "chunk-A", found.Segments[0].BlobID)
	require.Equal(t, "chunk-B", found.Segments[1].BlobID)
}

func TestListAllObjectsPropagatesCoalesced(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "bkt2"))

	segs := []storage.SegmentRef{
		{BlobID: "chunk-A", Size: 10},
		{BlobID: "chunk-B", Size: 20},
	}
	coal := []CoalescedShardRef{
		{CoalescedID: "coalesced-blob-1"},
	}
	seedObjectWithSegments(t, b, "bkt2", "obj", "v1", segs, coal)

	objs, err := b.ListAllObjects()
	require.NoError(t, err)

	var found *storage.SnapshotObject
	for i := range objs {
		if objs[i].Key == "obj" {
			found = &objs[i]
			break
		}
	}
	require.NotNil(t, found, "object obj not found in ListAllObjects")
	require.Len(t, found.Segments, 2)
	require.Len(t, found.Coalesced, 1, "Coalesced propagation gap")
	require.Equal(t, "coalesced-blob-1", found.Coalesced[0].CoalescedID)
}
