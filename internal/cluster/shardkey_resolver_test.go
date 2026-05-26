package cluster

import (
	"context"
	"fmt"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// seedObjectMetaVersion writes one objectMeta version directly into the FSM,
// mirroring applyPutObjectMeta's encryption-aware write path so the resolver's
// scan/unmarshal/(decrypt) path is exercised for real.
func seedObjectMetaVersion(t *testing.T, b *DistributedBackend, bucket, key, versionID string, m objectMeta) {
	t.Helper()
	m.Key = key
	f := b.FSMRef()
	val, err := marshalObjectMeta(m)
	require.NoError(t, err)
	require.NoError(t, f.db.Update(func(txn *badger.Txn) error {
		return f.setValue(txn, f.keys.ObjectMetaKeyV(bucket, key, versionID), val)
	}))
}

func TestResolveShardKeyPlacement(t *testing.T) {
	ctx := context.Background()
	backend := NewSingletonBackendForTest(t)
	require.NoError(t, backend.CreateBucket(ctx, "b"))

	// Object "obj" with two versions; the NON-latest version (v1) owns the EC
	// segment blob "seg-blob-1" and a coalesced shard. Latest (v2) is plain.
	segNodes := []string{"n1", "n2", "n3"}
	coalNodes := []string{"n4", "n5"}
	seedObjectMetaVersion(t, backend, "b", "obj", "v1", objectMeta{
		Segments: []storage.SegmentRef{
			{BlobID: "seg-blob-1", ECData: 2, ECParity: 1, NodeIDs: segNodes},
			{BlobID: "local-blob", ECData: 0, NodeIDs: []string{"nx"}}, // owner-local, excluded
		},
		Coalesced: []CoalescedShardRef{
			{CoalescedID: "c1", ShardKey: "obj/coalesced/c1", ECData: 2, ECParity: 1, NodeIDs: coalNodes},
		},
	})
	seedObjectMetaVersion(t, backend, "b", "obj", "v2", objectMeta{})

	// Object-version path fixture: v2 has EC placement on its own meta.
	ovNodes := []string{"o1", "o2"}
	seedObjectMetaVersion(t, backend, "b", "ov", "ovver", objectMeta{
		ECData: 2, ECParity: 1, NodeIDs: ovNodes,
	})

	// Prefix-leak: nested sibling "obj/bar" with its own EC segment that must
	// NOT match "obj"'s segment resolution.
	seedObjectMetaVersion(t, backend, "b", "obj/bar", "bv1", objectMeta{
		Segments: []storage.SegmentRef{
			{BlobID: "nested-blob", ECData: 4, ECParity: 2, NodeIDs: []string{"z1"}},
		},
	})

	// Marker-collision: an object literally keyed "foo/segments/bar" (no
	// segment named "bar" exists anywhere) → resolving its shard key is stale.
	seedObjectMetaVersion(t, backend, "b", "foo/segments/bar", "fv1", objectMeta{})

	// Trailing-slash boundary: an object "objbar" (no slash) shares the "obj"
	// prefix but must NOT leak into a scan resolving "obj"'s segments because
	// the scan prefix is "obj:b/obj/" (trailing slash).
	seedObjectMetaVersion(t, backend, "b", "objbar", "obv1", objectMeta{
		Segments: []storage.SegmentRef{
			{BlobID: "objbar-blob", ECData: 4, ECParity: 2, NodeIDs: []string{"y1"}},
		},
	})

	t.Run("segment on non-latest version", func(t *testing.T) {
		scan := NewShardKeyPlacementScanCache()
		rec, skip, err := backend.ResolveShardKeyPlacement(ctx, "b", "obj/segments/seg-blob-1", scan)
		require.NoError(t, err)
		require.Empty(t, skip)
		require.Equal(t, PlacementRecord{Nodes: segNodes, K: 2, M: 1}, rec)
	})

	t.Run("coalesced matched by ShardKey", func(t *testing.T) {
		scan := NewShardKeyPlacementScanCache()
		rec, skip, err := backend.ResolveShardKeyPlacement(ctx, "b", "obj/coalesced/c1", scan)
		require.NoError(t, err)
		require.Empty(t, skip)
		require.Equal(t, PlacementRecord{Nodes: coalNodes, K: 2, M: 1}, rec)
	})

	t.Run("owner-local append blob excluded", func(t *testing.T) {
		scan := NewShardKeyPlacementScanCache()
		_, skip, err := backend.ResolveShardKeyPlacement(ctx, "b", "obj/segments/local-blob", scan)
		require.NoError(t, err)
		require.Equal(t, "stale", skip)
	})

	t.Run("object absent or no matching ref is stale", func(t *testing.T) {
		scan := NewShardKeyPlacementScanCache()
		_, skip, err := backend.ResolveShardKeyPlacement(ctx, "b", "obj/segments/no-such-blob", scan)
		require.NoError(t, err)
		require.Equal(t, "stale", skip)

		_, skip, err = backend.ResolveShardKeyPlacement(ctx, "b", "ghost/segments/x", scan)
		require.NoError(t, err)
		require.Equal(t, "stale", skip)
	})

	t.Run("marker-collision object key is stale", func(t *testing.T) {
		scan := NewShardKeyPlacementScanCache()
		// Classifies to objectKey="foo", segment id="bar". No EC segment "bar".
		_, skip, err := backend.ResolveShardKeyPlacement(ctx, "b", "foo/segments/bar", scan)
		require.NoError(t, err)
		require.Equal(t, "stale", skip)
	})

	t.Run("prefix-leak guard does not match nested sibling", func(t *testing.T) {
		scan := NewShardKeyPlacementScanCache()
		// Resolving "obj"'s segment must not see "obj/bar"'s nested-blob.
		_, skip, err := backend.ResolveShardKeyPlacement(ctx, "b", "obj/segments/nested-blob", scan)
		require.NoError(t, err)
		require.Equal(t, "stale", skip)
		// And the nested object resolves its own blob.
		rec, skip, err := backend.ResolveShardKeyPlacement(ctx, "b", "obj/bar/segments/nested-blob", scan)
		require.NoError(t, err)
		require.Empty(t, skip)
		require.Equal(t, PlacementRecord{Nodes: []string{"z1"}, K: 4, M: 2}, rec)
	})

	t.Run("trailing-slash prefix boundary does not leak objbar", func(t *testing.T) {
		scan := NewShardKeyPlacementScanCache()
		// Resolving "obj"'s segment "objbar-blob" must not pick up "objbar"'s ref.
		_, skip, err := backend.ResolveShardKeyPlacement(ctx, "b", "obj/segments/objbar-blob", scan)
		require.NoError(t, err)
		require.Equal(t, "stale", skip)
		// "objbar" resolves its own blob.
		rec, skip, err := backend.ResolveShardKeyPlacement(ctx, "b", "objbar/segments/objbar-blob", scan)
		require.NoError(t, err)
		require.Empty(t, skip)
		require.Equal(t, PlacementRecord{Nodes: []string{"y1"}, K: 4, M: 2}, rec)
	})

	t.Run("object-version path delegates to LookupObjectPlacement", func(t *testing.T) {
		scan := NewShardKeyPlacementScanCache()
		rec, skip, err := backend.ResolveShardKeyPlacement(ctx, "b", "ov/ovver", scan)
		require.NoError(t, err)
		require.Empty(t, skip)
		require.Equal(t, PlacementRecord{Nodes: ovNodes, K: 2, M: 1}, rec)
	})

	t.Run("object-version path for missing or non-EC object is stale", func(t *testing.T) {
		scan := NewShardKeyPlacementScanCache()
		// "obj" version "v2" was seeded without EC placement → empty record → stale.
		_, skip, err := backend.ResolveShardKeyPlacement(ctx, "b", "obj/v2", scan)
		require.NoError(t, err)
		require.Equal(t, "stale", skip)
		// Wholly absent object version → stale.
		_, skip, err = backend.ResolveShardKeyPlacement(ctx, "b", "ghost/nover", scan)
		require.NoError(t, err)
		require.Equal(t, "stale", skip)
	})

	t.Run("cache hit avoids rescan", func(t *testing.T) {
		scan := NewShardKeyPlacementScanCache()
		_, _, err := backend.ResolveShardKeyPlacement(ctx, "b", "obj/segments/seg-blob-1", scan)
		require.NoError(t, err)
		require.Equal(t, 1, scan.scans)
		// Different shard of the same object → reuse, no rescan.
		_, _, err = backend.ResolveShardKeyPlacement(ctx, "b", "obj/coalesced/c1", scan)
		require.NoError(t, err)
		require.Equal(t, 1, scan.scans)
		// A miss on the same object also must not rescan (negative cached).
		_, skip, err := backend.ResolveShardKeyPlacement(ctx, "b", "obj/segments/no-such-blob", scan)
		require.NoError(t, err)
		require.Equal(t, "stale", skip)
		require.Equal(t, 1, scan.scans)
	})
}

func TestResolveShardKeyPlacement_Capped(t *testing.T) {
	ctx := context.Background()
	backend := NewSingletonBackendForTest(t)
	require.NoError(t, backend.CreateBucket(ctx, "b"))

	// Seed > cap exact-key versions of one object. The EC segment lives on the
	// first version, but the cap aborts the scan before the map matters.
	scan := NewShardKeyPlacementScanCache()
	scan.cap = 3
	for i := 0; i < scan.cap+2; i++ {
		seedObjectMetaVersion(t, backend, "b", "big", fmt.Sprintf("v%03d", i), objectMeta{
			Segments: []storage.SegmentRef{
				{BlobID: "seg-x", ECData: 2, ECParity: 1, NodeIDs: []string{"n1", "n2", "n3"}},
			},
		})
	}

	_, skip, err := backend.ResolveShardKeyPlacement(ctx, "b", "big/segments/seg-x", scan)
	require.NoError(t, err)
	require.Equal(t, "placement_scan_capped", skip)
}

func TestResolveShardKeyPlacement_NilCache(t *testing.T) {
	ctx := context.Background()
	backend := NewSingletonBackendForTest(t)
	require.NoError(t, backend.CreateBucket(ctx, "b"))
	seedObjectMetaVersion(t, backend, "b", "obj", "v1", objectMeta{
		Segments: []storage.SegmentRef{
			{BlobID: "seg-1", ECData: 2, ECParity: 1, NodeIDs: []string{"n1", "n2", "n3"}},
		},
	})

	// scan == nil scans without caching and still resolves correctly.
	rec, skip, err := backend.ResolveShardKeyPlacement(ctx, "b", "obj/segments/seg-1", nil)
	require.NoError(t, err)
	require.Empty(t, skip)
	require.Equal(t, PlacementRecord{Nodes: []string{"n1", "n2", "n3"}, K: 2, M: 1}, rec)
}
