package cluster

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/transport"
	"github.com/stretchr/testify/require"
)

// writeVerBlob writes a per-version quorum-meta blob for (bucket,key,vid) to the
// given ShardService's local versions subtree.
func writeVerBlob(t *testing.T, svc *ShardService, bucket, key, vid string, c PutObjectMetaCmd) {
	t.Helper()
	c.Bucket = bucket
	c.Key = key
	c.VersionID = vid
	blob, err := EncodeCommand(CmdPutObjectMeta, c)
	require.NoError(t, err)
	require.NoError(t, svc.writeQuorumMetaVersionLocal(bucket, filepath.Join(key, vid), blob))
}

// TestScanQuorumMetaVersionsBucket_GroupsByDecodedKey proves the walker returns
// ONE max-vid entry per distinct decoded cmd.Key. The critical case: nested keys
// "a/b" and "a/b/c.txt" — the dir ".quorum_meta_versions/{bkt}/a/b/" is BOTH a
// key-leaf dir (holds a/b's vid files) AND an intermediate dir (holds c.txt/), so
// grouping must be by decoded cmd.Key, not dir structure.
func TestScanQuorumMetaVersionsBucket_GroupsByDecodedKey(t *testing.T) {
	b := newTestDistributedBackend(t)
	svc := b.shardSvc

	// key "a/b": v1 then v2 (newer) — max-vid must win.
	writeVerBlob(t, svc, "bkt", "a/b", "019ed400-0000-7000-8000-000000000001", PutObjectMetaCmd{ETag: "ab-v1"})
	writeVerBlob(t, svc, "bkt", "a/b", "019ed400-0000-7000-8000-000000000002", PutObjectMetaCmd{ETag: "ab-v2"})
	// key "a/b/c.txt": lives UNDER a/b's leaf dir — must be its own key.
	writeVerBlob(t, svc, "bkt", "a/b/c.txt", "019ed400-0000-7000-8000-000000000003", PutObjectMetaCmd{ETag: "abc-v1"})
	// key "x": local-max is a delete marker — walker MUST still return it (merge excludes later).
	writeVerBlob(t, svc, "bkt", "x", "019ed400-0000-7000-8000-000000000004", PutObjectMetaCmd{ETag: "x-v1"})
	writeVerBlob(t, svc, "bkt", "x", "019ed400-0000-7000-8000-000000000005", PutObjectMetaCmd{IsDeleteMarker: true})

	got, err := svc.ScanQuorumMetaVersionsBucket("bkt", "")
	require.NoError(t, err)

	byKey := map[string]PutObjectMetaCmd{}
	for _, c := range got {
		_, dup := byKey[c.Key]
		require.False(t, dup, "key %s returned more than once", c.Key)
		byKey[c.Key] = c
	}
	require.Len(t, byKey, 3, "three distinct keys: a/b, a/b/c.txt, x")
	require.Equal(t, "ab-v2", byKey["a/b"].ETag, "a/b max-vid = v2")
	require.Equal(t, "abc-v1", byKey["a/b/c.txt"].ETag, "a/b/c.txt is its own key (not folded into a/b)")
	require.True(t, byKey["x"].IsDeleteMarker, "x local-max is the marker (walker includes markers)")
}

// TestScanQuorumMetaVersionsBucket_PrefixFilter proves the prefix filter is on
// the decoded cmd.Key.
func TestScanQuorumMetaVersionsBucket_PrefixFilter(t *testing.T) {
	b := newTestDistributedBackend(t)
	svc := b.shardSvc
	writeVerBlob(t, svc, "bkt", "foo/1", "019ed400-0000-7000-8000-000000000001", PutObjectMetaCmd{})
	writeVerBlob(t, svc, "bkt", "foo/2", "019ed400-0000-7000-8000-000000000002", PutObjectMetaCmd{})
	writeVerBlob(t, svc, "bkt", "bar/1", "019ed400-0000-7000-8000-000000000003", PutObjectMetaCmd{})

	got, err := svc.ScanQuorumMetaVersionsBucket("bkt", "foo/")
	require.NoError(t, err)
	keys := map[string]bool{}
	for _, c := range got {
		keys[c.Key] = true
	}
	require.Equal(t, map[string]bool{"foo/1": true, "foo/2": true}, keys)
}

// listKeys returns the sorted object keys ListObjects reports.
func listKeys(t *testing.T, b *DistributedBackend, ctx context.Context, bkt, prefix string) []string {
	t.Helper()
	objs, err := b.ListObjects(ctx, bkt, prefix, 1000)
	require.NoError(t, err)
	keys := make([]string, 0, len(objs))
	for _, o := range objs {
		keys = append(keys, o.Key)
	}
	return keys
}

// TestListObjects_PerVersionDerive_DivergenceRepro is the core S2b repro: on a
// versioning-enabled bucket, PUT v1, PUT v2, then hard-delete v2
// (DeleteObjectVersion) → ListObjects must show the key with v1, matching
// HEAD/GET. RED before the per-version derive branch: legacy scatterGatherList
// reads the stale latest-only blob (still v2, not maintained on hard-delete) so
// the deleted-latest object stays visible. Then delete v1 too → key absent.
func TestListObjects_PerVersionDerive_DivergenceRepro(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	const bkt, key = "vbkt", "obj"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))

	vid1 := putVersioned(t, b, ctx, bkt, key, "content-v1")
	vid2 := putVersioned(t, b, ctx, bkt, key, "content-v2")

	// Sanity: HEAD agrees latest is v2 before the delete.
	head, err := b.HeadObject(ctx, bkt, key)
	require.NoError(t, err)
	require.Equal(t, vid2, head.VersionID)

	// Hard-delete the latest (v2). HEAD now re-derives v1.
	require.NoError(t, b.DeleteObjectVersion(bkt, key, vid2))
	head, err = b.HeadObject(ctx, bkt, key)
	require.NoError(t, err)
	require.Equal(t, vid1, head.VersionID)

	// LIST must agree: key present (it is v1's live latest), NOT omitted, NOT v2.
	objs, err := b.ListObjects(ctx, bkt, "", 1000)
	require.NoError(t, err)
	require.Len(t, objs, 1, "RED before derive: stale latest-only blob keeps the hard-deleted object visible")
	require.Equal(t, key, objs[0].Key)
	require.Equal(t, vid1, objs[0].VersionID, "LIST must show the re-derived latest v1, matching HEAD")

	// Delete v1 too → key absent from LIST (no live version remains).
	require.NoError(t, b.DeleteObjectVersion(bkt, key, vid1))
	require.Empty(t, listKeys(t, b, ctx, bkt, ""), "after deleting every version, key is absent from LIST")
}

// TestListObjects_PerVersionDerive_SoftDeleteExcluded proves a soft-delete
// (DeleteObject writes a delete marker as the newest per-version blob) omits the
// key from LIST.
func TestListObjects_PerVersionDerive_SoftDeleteExcluded(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	const bkt, key = "vbkt", "obj"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))

	_ = putVersioned(t, b, ctx, bkt, key, "content-v1")
	require.NoError(t, b.DeleteObject(ctx, bkt, key)) // soft-delete → marker as latest

	require.Empty(t, listKeys(t, b, ctx, bkt, ""), "soft-deleted key (marker latest) omitted from LIST")
}

// TestListObjects_PerVersionDerive_DedupAndPagination proves the derive dedups
// multiple versions of a key to one (latest) and that prefix/maxKeys/marker
// pagination is preserved (byte-compatible with scatterGatherList's contract).
func TestListObjects_PerVersionDerive_DedupAndPagination(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	const bkt = "vbkt"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))

	// Three keys, "a" with three versions (dedup to one), under two prefixes.
	for _, body := range []string{"a1", "a2", "a3"} {
		_ = putVersioned(t, b, ctx, bkt, "p/a", body)
	}
	_ = putVersioned(t, b, ctx, bkt, "p/b", "b1")
	_ = putVersioned(t, b, ctx, bkt, "q/c", "c1")

	// Dedup: p/a appears once despite 3 versions.
	require.Equal(t, []string{"p/a", "p/b", "q/c"}, listKeys(t, b, ctx, bkt, ""), "each key once (deduped)")

	// Prefix filter.
	require.Equal(t, []string{"p/a", "p/b"}, listKeys(t, b, ctx, bkt, "p/"))

	// maxKeys cap.
	capped, err := b.ListObjects(ctx, bkt, "", 2)
	require.NoError(t, err)
	require.Len(t, capped, 2, "maxKeys honored")

	// Marker pagination: page after "p/a" yields p/b, q/c (truncation respected).
	page, truncated, err := b.ListObjectsPage(ctx, bkt, "", "p/a", 1)
	require.NoError(t, err)
	require.Equal(t, []string{"p/b"}, objKeys(page))
	require.True(t, truncated, "more entries remain after p/b")
}

// TestListObjects_LegacyFallbackNonVersioned proves a NON-versioned bucket still
// lists via the legacy scatterGatherList path (unchanged): a plain PUT is visible.
func TestListObjects_LegacyFallbackNonVersioned(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	const bkt, key = "plainbkt", "obj"
	require.NoError(t, b.CreateBucket(ctx, bkt)) // no SetBucketVersioning → legacy path

	_ = putVersioned(t, b, ctx, bkt, key, "body") // PutObject works on non-versioned too
	require.Equal(t, []string{key}, listKeys(t, b, ctx, bkt, ""), "non-versioned LIST unchanged (legacy)")
}

// TestListObjects_CtxFlagFalseForcesLegacy pins the activation boundary the other
// way: a versioning-enabled bucket whose ctx flag is explicitly stamped FALSE
// (e.g. a suspended decision propagated from the edge) takes the legacy
// scatterGatherList path, so a hard-deleted-latest stays visible via the stale
// latest-only blob — proving the branch reads the ctx flag, not just local state.
func TestListObjects_CtxFlagFalseForcesLegacy(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	const bkt, key = "vbkt", "obj"
	enabled := context.Background()
	require.NoError(t, b.CreateBucket(enabled, bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))

	_ = putVersioned(t, b, enabled, bkt, key, "content-v1")
	vid2 := putVersioned(t, b, enabled, bkt, key, "content-v2")
	require.NoError(t, b.DeleteObjectVersion(bkt, key, vid2)) // hard-delete latest

	// ctx flag FALSE → legacy path → stale latest-only blob (v2) still visible.
	legacyCtx := ContextWithBucketVersioning(context.Background(), false)
	objs, err := b.ListObjects(legacyCtx, bkt, "", 1000)
	require.NoError(t, err)
	require.Len(t, objs, 1, "legacy path returns the stale latest-only entry")
	require.Equal(t, vid2, objs[0].VersionID, "ctx-flag-false forces legacy → shows the hard-deleted latest (boundary pinned)")
}

// objKeys extracts keys from a []*storage.Object page.
func objKeys(objs []*storage.Object) []string {
	keys := make([]string, 0, len(objs))
	for _, o := range objs {
		keys = append(keys, o.Key)
	}
	return keys
}

// TestScanQuorumMetaVersions_RPC proves the per-version bucket walker RPC
// enumerates a remote node's per-version blobs grouped by key.
func TestScanQuorumMetaVersions_RPC(t *testing.T) {
	ctx := context.Background()
	keeper, clusterID := testDEKKeeper(t)

	trA := transport.MustNewHTTPTransport("test-cluster-psk")
	trB := transport.MustNewHTTPTransport("test-cluster-psk")
	require.NoError(t, trA.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, trB.Listen(ctx, "127.0.0.1:0"))
	defer trA.Close()
	defer trB.Close()

	svcA := NewShardService(t.TempDir(), trA, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))
	svcB := NewShardService(t.TempDir(), trB, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))
	trB.RegisterBufferedRoute(transport.RouteShardRPC, svcB.NativeRPCHandler())

	writeVerBlob(t, svcB, "bkt", "k1", "019ed400-0000-7000-8000-000000000001", PutObjectMetaCmd{ETag: "k1-v1"})
	writeVerBlob(t, svcB, "bkt", "k1", "019ed400-0000-7000-8000-000000000002", PutObjectMetaCmd{ETag: "k1-v2"})
	writeVerBlob(t, svcB, "bkt", "k2", "019ed400-0000-7000-8000-000000000003", PutObjectMetaCmd{ETag: "k2-v1"})

	got, err := svcA.ScanQuorumMetaVersions(ctx, trB.LocalAddr(), "bkt", "")
	require.NoError(t, err)
	byKey := map[string]string{}
	for _, c := range got {
		byKey[c.Key] = c.ETag
	}
	require.Equal(t, map[string]string{"k1": "k1-v2", "k2": "k2-v1"}, byKey, "max-vid per key over the wire")
}
