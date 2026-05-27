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

// TestListAllObjectsStrictFailsClosedOnCorruptMeta proves the GC known-set fix:
// a valid object plus an undecodable obj: record. The tolerant ListAllObjects
// silently skips the corrupt record (returns nil error, 1 object); the strict
// ListAllObjectsStrict fails closed (returns an error) so the scrubber skips its
// sweep rather than treating the corrupt object's segments as orphaned.
func TestListAllObjectsStrictFailsClosedOnCorruptMeta(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "bkt"))

	// One valid object so the tolerant path returns 1 (not 0) — a stronger assertion.
	seedObjectWithSegments(t, b, "bkt", "good", "v1",
		[]storage.SegmentRef{{BlobID: "chunk-A", Size: 10}}, nil)

	// Inject a properly-keyed obj: record with an undecodable value.
	require.NoError(t, b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(b.ks().ObjectMetaKeyV("bkt", "corrupt", "v1"), []byte{0xff, 0xff, 0xff})
	}))

	tolerant, err := b.ListAllObjects()
	require.NoError(t, err, "tolerant ListAllObjects must skip the corrupt record")
	var keys []string
	for _, o := range tolerant {
		keys = append(keys, o.Key)
	}
	require.Equal(t, []string{"good"}, keys, "tolerant must return only the valid object")

	_, err = b.ListAllObjectsStrict()
	require.Error(t, err, "strict ListAllObjectsStrict must fail closed on corrupt meta")
	require.Contains(t, err.Error(), "gc known-set")
}
