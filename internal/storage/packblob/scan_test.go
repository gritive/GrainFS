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
