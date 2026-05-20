package packblob

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// newTestPackedBackendWithInner creates a PackedBackend and also returns the
// inner LocalBackend so tests can inject objects directly into the inner layer.
func newTestPackedBackendWithInner(t *testing.T) (*PackedBackend, storage.Backend) {
	t.Helper()
	dir := t.TempDir()
	inner, err := storage.NewLocalBackend(dir + "/local")
	require.NoError(t, err)
	pb, err := NewPackedBackend(inner, dir+"/blobs", 64*1024) // 64 KiB threshold
	require.NoError(t, err)
	t.Cleanup(func() { pb.Close() })
	return pb, inner
}

// scannerOf type-asserts pb to the ScanObjectsGrouped interface.
// The method is NOT on storage.Backend; it is an optional extension.
func scannerOf(t *testing.T, pb *PackedBackend) interface {
	ScanObjectsGrouped(bucket string) (<-chan storage.ObjectKeyGroup, error)
} {
	t.Helper()
	s, ok := any(pb).(interface {
		ScanObjectsGrouped(bucket string) (<-chan storage.ObjectKeyGroup, error)
	})
	require.True(t, ok, "PackedBackend must implement ScanObjectsGrouped")
	return s
}

// TestPackedBackend_ScanObjectsGroupedFusesPackedAndInner guards against R3:
// PackedBackend.ScanObjectsGrouped must enumerate both packed-only objects
// (below pack-threshold) AND inner-backend objects (above pack-threshold).
func TestPackedBackend_ScanObjectsGroupedFusesPackedAndInner(t *testing.T) {
	pb := newTestPackedBackend(t)
	ctx := context.Background()
	const bucket = "b"
	require.NoError(t, pb.CreateBucket(ctx, bucket))

	smallBody := []byte("small")
	_, err := pb.PutObject(ctx, bucket, "small-key", bytes.NewReader(smallBody), "text/plain")
	require.NoError(t, err)

	bigBody := bytes.Repeat([]byte("a"), 70*1024) // exceeds 64 KiB threshold
	_, err = pb.PutObject(ctx, bucket, "big-key", bytes.NewReader(bigBody), "text/plain")
	require.NoError(t, err)

	ch, err := scannerOf(t, pb).ScanObjectsGrouped(bucket)
	require.NoError(t, err)

	var groups []storage.ObjectKeyGroup
	for g := range ch {
		groups = append(groups, g)
	}

	keys := make(map[string]bool)
	for _, g := range groups {
		keys[g.Key] = true
		if g.Key == "small-key" {
			require.Len(t, g.Versions, 1)
			require.True(t, g.Versions[0].IsLatest)
			require.False(t, g.Versions[0].IsDeleteMarker)
			require.Equal(t, "", g.Versions[0].VersionID)
		}
	}
	require.True(t, keys["small-key"], "packed-only key must be enumerated")
	require.True(t, keys["big-key"], "above-threshold key must be enumerated via inner delegate")
}

// TestPackedBackend_ListBucketsFusesPackedAndInner guards against R3:
// ListBuckets must include both buckets visible in inner and buckets
// that only appear in the in-memory packed index (e.g. after an index
// load where the inner bucket was cleaned up out-of-band).
//
// Because the public API enforces that every bucket exists in inner
// (PutObjectWithRequest calls HeadBucket before packing), the "index-only"
// scenario is simulated by directly injecting an entry into pb.index.
func TestPackedBackend_ListBucketsFusesPackedAndInner(t *testing.T) {
	pb := newTestPackedBackend(t)
	ctx := context.Background()

	// bucket-a: created and written normally → visible in inner
	require.NoError(t, pb.CreateBucket(ctx, "bucket-a"))
	bigBody := bytes.Repeat([]byte("x"), 70*1024)
	_, err := pb.PutObject(ctx, "bucket-a", "k", bytes.NewReader(bigBody), "text/plain")
	require.NoError(t, err)

	// bucket-c: injected directly into the packed index only (no inner bucket).
	// This simulates index drift (e.g. LoadIndex after inner cleanup).
	e := &indexEntry{OriginalSize: 5, ETag: "abc"}
	e.Refcount.Add(1)
	pb.index.Store(packedKey{bucket: "bucket-c", key: "k"}, e)

	buckets, err := pb.ListBuckets(ctx)
	require.NoError(t, err)

	set := make(map[string]bool)
	for _, b := range buckets {
		set[b] = true
	}
	require.True(t, set["bucket-a"], "inner bucket must appear")
	require.True(t, set["bucket-c"], "index-only bucket must appear after fuse")
}

// TestPackedBackend_DeleteObjectReturningMarker_CleanupPackedIndex_NonVersioning
// guards against R5: when DeleteObjectReturningMarker is invoked on a
// non-versioning bucket containing a packed object, the packed index entry must
// be evicted so a subsequent HeadObject does not find the object alive.
// Operations.DeleteObjectReturningMarker enters the versionedSoftDeleter branch
// without a bucket-level versioning check, so PackedBackend.DeleteObjectReturningMarker
// is called on both versioning-enabled AND non-versioning buckets.
func TestPackedBackend_DeleteObjectReturningMarker_CleanupPackedIndex_NonVersioning(t *testing.T) {
	pb := newTestPackedBackend(t)
	ctx := context.Background()
	const bucket = "b"
	require.NoError(t, pb.CreateBucket(ctx, bucket))

	// Small body → goes into packed index (below 64 KiB threshold)
	body := []byte("small")
	_, err := pb.PutObject(ctx, bucket, "k", bytes.NewReader(body), "text/plain")
	require.NoError(t, err)

	// Verify it's alive in packed index
	_, err = pb.HeadObject(ctx, bucket, "k")
	require.NoError(t, err, "packed object must be alive before delete")

	// Delete via the marker-returning path (lifecycle worker's actual surface)
	marker, err := pb.DeleteObjectReturningMarker(bucket, "k")
	require.NoError(t, err)
	require.Equal(t, "", marker, "non-versioning bucket must return empty marker")

	// After delete, HeadObject must NOT find the object (R5 guard)
	_, err = pb.HeadObject(ctx, bucket, "k")
	require.Error(t, err, "packed index entry must be evicted on DeleteObjectReturningMarker (R5 guard)")
}

// TestPackedBackend_ScanObjectsGroupedDedupBranchPrefersPacked guards against
// the case where the same key exists in both the packed index and inner. The
// packed entry must win and only one group must be emitted (no duplicate).
func TestPackedBackend_ScanObjectsGroupedDedupBranchPrefersPacked(t *testing.T) {
	pb, inner := newTestPackedBackendWithInner(t)
	ctx := context.Background()
	const bucket = "b"
	require.NoError(t, pb.CreateBucket(ctx, bucket))

	// Write directly to inner (simulates invariant violation: same key in both).
	innerBody := []byte("inner-body")
	_, err := inner.PutObject(ctx, bucket, "k", bytes.NewReader(innerBody), "text/plain")
	require.NoError(t, err)

	// Write via PackedBackend so it lands in the packed index.
	packedBody := []byte("packed")
	_, err = pb.PutObject(ctx, bucket, "k", bytes.NewReader(packedBody), "text/plain")
	require.NoError(t, err)

	ch, err := scannerOf(t, pb).ScanObjectsGrouped(bucket)
	require.NoError(t, err)
	var groups []storage.ObjectKeyGroup
	for g := range ch {
		groups = append(groups, g)
	}

	count := 0
	for _, g := range groups {
		if g.Key == "k" {
			count++
			require.Equal(t, "", g.Versions[0].VersionID, "packed wins (unversioned)")
		}
	}
	require.Equal(t, 1, count, "exactly one group for 'k' — dedup branch must skip inner duplicate")
}
