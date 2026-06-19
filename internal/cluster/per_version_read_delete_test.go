package cluster

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/transport"
	"github.com/stretchr/testify/require"
)

// putVersioned PUTs a body on a versioning-enabled bucket and returns the vid.
func putVersioned(t *testing.T, b *DistributedBackend, ctx context.Context, bkt, key, body string) string {
	t.Helper()
	sz := int64(len(body))
	obj, err := b.PutObjectWithRequest(ctx, storage.PutObjectRequest{
		Bucket: bkt, Key: key, Body: bytes.NewReader([]byte(body)),
		ContentType: "text/plain", SizeHint: &sz,
	})
	require.NoError(t, err)
	require.NotEmpty(t, obj.VersionID)
	return obj.VersionID
}

// TestReadQuorumMetaVersionsLocal_ListsKeyVersions proves the local lister
// enumerates every per-version blob under .quorum_meta_versions/{bucket}/{key}/.
func TestReadQuorumMetaVersionsLocal_ListsKeyVersions(t *testing.T) {
	b := newTestDistributedBackend(t)
	// Two version blobs for one key, written via the S1 local primitive.
	for _, vid := range []string{"v1", "v2"} {
		blob, err := EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{Bucket: "bkt", Key: "a/b/c.txt", VersionID: vid, ETag: "e-" + vid})
		require.NoError(t, err)
		require.NoError(t, b.shardSvc.writeQuorumMetaVersionLocal("bkt", filepath.Join("a/b/c.txt", vid), blob, 0))
	}
	cmds, err := b.shardSvc.readQuorumMetaVersionsLocal("bkt", "a/b/c.txt")
	require.NoError(t, err)
	got := map[string]string{}
	for _, c := range cmds {
		got[c.VersionID] = c.ETag
	}
	require.Equal(t, map[string]string{"v1": "e-v1", "v2": "e-v2"}, got)
}

// TestReadQuorumMetaVersions_RPC proves the per-version list RPC enumerates a
// remote placement node's per-version blobs for a key.
func TestReadQuorumMetaVersions_RPC(t *testing.T) {
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

	for _, vid := range []string{"v1", "v2"} {
		blob, err := EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{Bucket: "bkt", Key: "k", VersionID: vid, ETag: "e-" + vid})
		require.NoError(t, err)
		require.NoError(t, svcB.writeQuorumMetaVersionLocal("bkt", filepath.Join("k", vid), blob, 0))
	}

	cmds, err := svcA.ReadQuorumMetaVersions(ctx, trB.LocalAddr(), "bkt", "k")
	require.NoError(t, err)
	got := map[string]string{}
	for _, c := range cmds {
		got[c.VersionID] = c.ETag
	}
	require.Equal(t, map[string]string{"v1": "e-v1", "v2": "e-v2"}, got)
}

// TestReadQuorumMetaVersions_UnionsAcrossGroups proves the backend fan-out
// unions a key's per-version blobs across ALL placement groups (spanning
// generations), not just the bucket-routed group: v1 on group g1's node A, v2 on
// group g2's node B → both returned; deriveLatestVersion picks v2 (max VID).
func TestReadQuorumMetaVersions_UnionsAcrossGroups(t *testing.T) {
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
	trA.RegisterBufferedRoute(transport.RouteShardRPC, svcA.NativeRPCHandler())
	trB.RegisterBufferedRoute(transport.RouteShardRPC, svcB.NativeRPCHandler())

	write := func(svc *ShardService, vid string) {
		t.Helper()
		blob, err := EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{Bucket: "bkt", Key: "k", VersionID: vid, ETag: "e-" + vid})
		require.NoError(t, err)
		require.NoError(t, svc.writeQuorumMetaVersionLocal("bkt", filepath.Join("k", vid), blob, 0))
	}
	// v1 on node A (group g1), v2 on node B (group g2 — a second generation).
	write(svcA, "v1")
	write(svcB, "v2")

	backendA := &DistributedBackend{
		selfAddr: trA.LocalAddr(),
		shardSvc: svcA,
		shardGroup: &fakeShardGroupSource{groups: map[string]ShardGroupEntry{
			"g1": {ID: "g1", PeerIDs: []string{trA.LocalAddr()}},
			"g2": {ID: "g2", PeerIDs: []string{trB.LocalAddr()}},
		}},
	}

	cmds, err := backendA.readQuorumMetaVersions("bkt", "k")
	require.NoError(t, err)
	got := map[string]string{}
	for _, c := range cmds {
		got[c.VersionID] = c.ETag
	}
	require.Equal(t, map[string]string{"v1": "e-v1", "v2": "e-v2"}, got, "union must span all groups/generations")

	latest, live := deriveLatestVersion(cmds)
	require.True(t, live)
	require.Equal(t, "v2", latest.VersionID, "derive-latest = max VersionID")
}

// TestDeriveLatestVersion_MarkerAsLatestIsDeleted proves a delete-marker with
// the max VersionID makes the object's latest state "deleted" (not live).
func TestDeriveLatestVersion_MarkerAsLatestIsDeleted(t *testing.T) {
	cmds := []PutObjectMetaCmd{
		{VersionID: "v1", ETag: "e1"},
		{VersionID: "v2", IsDeleteMarker: true}, // marker is the newest
	}
	_, live := deriveLatestVersion(cmds)
	require.False(t, live, "delete marker as latest → object is deleted")

	// Empty set → not live either.
	_, liveEmpty := deriveLatestVersion(nil)
	require.False(t, liveEmpty)
}

// TestHeadObjectMeta_PerVersionHookOverridesLegacyLatest proves the read hook is
// authoritative: when the per-version store holds a NEWER version (v2) than the
// legacy latest-only blob (v1) — the situation after a cross-generation write or
// a per-version-store-ahead state — HeadObject derives v2, not the stale legacy
// v1. RED before the hook: headObjectMeta returns the legacy latest-only v1.
func TestHeadObjectMeta_PerVersionHookOverridesLegacyLatest(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	const bkt, key = "vbkt", "obj"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))

	// Legacy latest-only blob says v1 (older); per-version store has v1 AND a
	// newer v2 that the latest-only blob never saw.
	v1Blob, err := EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{Bucket: bkt, Key: key, VersionID: "019ed400-0000-7000-8000-000000000001", ETag: "etag-v1", NodeIDs: []string{"self"}, ECData: 1})
	require.NoError(t, err)
	require.NoError(t, b.shardSvc.writeQuorumMetaLocal(bkt, key, v1Blob, 0)) // legacy latest-only = v1
	require.NoError(t, b.shardSvc.writeQuorumMetaVersionLocal(bkt, filepath.Join(key, "019ed400-0000-7000-8000-000000000001"), v1Blob, 0))
	v2Blob, err := EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{Bucket: bkt, Key: key, VersionID: "019ed400-0000-7000-8000-000000000002", ETag: "etag-v2", NodeIDs: []string{"self"}, ECData: 1})
	require.NoError(t, err)
	require.NoError(t, b.shardSvc.writeQuorumMetaVersionLocal(bkt, filepath.Join(key, "019ed400-0000-7000-8000-000000000002"), v2Blob, 0))

	head, err := b.HeadObject(ctx, bkt, key)
	require.NoError(t, err)
	require.Equal(t, "etag-v2", head.ETag, "per-version derive must win over stale legacy latest-only blob")
}

// TestHeadObject_PerVersionDerivesLatest proves HeadObject derives latest by
// scan over per-version blobs: PUT v1, v2 → HeadObject returns v2.
func TestHeadObject_PerVersionDerivesLatest(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	const bkt, key = "vbkt", "obj"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))

	_ = putVersioned(t, b, ctx, bkt, key, "content-v1")
	vid2 := putVersioned(t, b, ctx, bkt, key, "content-v2")

	head, err := b.HeadObject(ctx, bkt, key)
	require.NoError(t, err)
	require.Equal(t, vid2, head.VersionID, "HEAD must derive latest = v2 from per-version blobs")
}

// TestGetObjectVersion_PerVersionDirectRead proves GetObjectVersion(v1) resolves
// the older version from its own per-version blob.
func TestGetObjectVersion_PerVersionDirectRead(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	const bkt, key = "vbkt", "obj"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))

	vid1 := putVersioned(t, b, ctx, bkt, key, "content-v1")
	_ = putVersioned(t, b, ctx, bkt, key, "content-v2")

	hv, err := b.HeadObjectVersion(context.Background(), bkt, key, vid1)
	require.NoError(t, err)
	require.Equal(t, vid1, hv.VersionID)

	rc, _, err := b.GetObjectVersion(context.Background(), bkt, key, vid1)
	require.NoError(t, err)
	got, _ := io.ReadAll(rc)
	rc.Close()
	require.Equal(t, []byte("content-v1"), got)
}

// TestHeadObject_DeleteMarkerAsLatest404 proves a soft-delete (delete marker is
// the newest per-version blob) makes HeadObject return 404.
func TestHeadObject_DeleteMarkerAsLatest404(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	const bkt, key = "vbkt", "obj"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))

	vid1 := putVersioned(t, b, ctx, bkt, key, "content-v1")
	require.NoError(t, b.DeleteObject(ctx, bkt, key)) // soft-delete → marker as latest

	_, err := b.HeadObject(ctx, bkt, key)
	require.ErrorIs(t, err, storage.ErrObjectNotFound, "delete marker as latest → HEAD 404")

	// The live older version is still readable by id.
	rc, _, err := b.GetObjectVersion(context.Background(), bkt, key, vid1)
	require.NoError(t, err)
	got, _ := io.ReadAll(rc)
	rc.Close()
	require.Equal(t, []byte("content-v1"), got)
}

// TestHeadObject_LegacyFallbackNoPerVersionBlob proves an object with zero
// per-version blobs (legacy/non-versioned) still resolves via the legacy
// quorum-meta path — behavior-safety for pre-S1 objects.
func TestHeadObject_LegacyFallbackNoPerVersionBlob(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	const bkt, key = "legacybkt", "obj"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	// Non-versioned bucket: PUT writes only the latest-only blob, no per-version blobs.
	body := "legacy-body"
	sz := int64(len(body))
	put, err := b.PutObjectWithRequest(ctx, storage.PutObjectRequest{
		Bucket: bkt, Key: key, Body: bytes.NewReader([]byte(body)),
		ContentType: "text/plain", SizeHint: &sz,
	})
	require.NoError(t, err)

	head, err := b.HeadObject(ctx, bkt, key)
	require.NoError(t, err, "legacy object (no per-version blob) must resolve via fallback")
	require.Equal(t, put.ETag, head.ETag)

	rc, _, err := b.GetObject(ctx, bkt, key)
	require.NoError(t, err)
	got, _ := io.ReadAll(rc)
	rc.Close()
	require.Equal(t, []byte(body), got)
}

// TestDeleteObjectVersion_DualDeletesPerVersionBlob proves DeleteObjectVersion
// also removes the per-version blob: after deleting v2, HeadObject re-derives v1,
// the v2 per-version blob is gone, and GetObjectVersion(v2) → 404.
func TestDeleteObjectVersion_DualDeletesPerVersionBlob(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	const bkt, key = "vbkt", "obj"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))

	vid1 := putVersioned(t, b, ctx, bkt, key, "content-v1")
	vid2 := putVersioned(t, b, ctx, bkt, key, "content-v2")

	require.NoError(t, b.DeleteObjectVersion(bkt, key, vid2))

	// HeadObject re-derives latest = v1 (v2's per-version blob gone).
	head, err := b.HeadObject(ctx, bkt, key)
	require.NoError(t, err)
	require.Equal(t, vid1, head.VersionID, "after hard-deleting v2, HEAD must re-derive v1")

	// The v2 per-version blob is no longer enumerated.
	cmds, err := b.readQuorumMetaVersions(bkt, key)
	require.NoError(t, err)
	for _, c := range cmds {
		require.NotEqual(t, vid2, c.VersionID, "v2 per-version blob must be deleted")
	}

	// GetObjectVersion(v2) → 404.
	_, _, gerr := b.GetObjectVersion(context.Background(), bkt, key, vid2)
	require.ErrorIs(t, gerr, storage.ErrObjectNotFound)
}

// TestGetObjectVersion_MixedEraPreS1FSMOnly proves a versioning-enabled key that
// straddles the S1 boundary still resolves a pre-S1 version that exists ONLY as a
// BadgerDB ObjectMetaKeyV FSM record (no per-version blob, because S1's blob write
// is versioning+post-S1 gated). The key ALSO has a post-S1 version WITH a
// per-version blob, so the buggy predicate (per-version MISS on a key with other
// blobs → 404) would 404 the pre-S1 version. RED: GET/HEAD of the pre-S1 version
// returns ErrObjectNotFound; GREEN after the FSM-only fallback fix.
func TestGetObjectVersion_MixedEraPreS1FSMOnly(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	const bkt, key = "vbkt", "obj"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))

	// Pre-S1 version: FSM ObjectMetaKeyV record ONLY, no per-version blob.
	// PreserveLatest avoids touching the latest pointer so the post-S1 PUT wins it.
	const preS1Vid = "019ed400-0000-7000-8000-000000000001"
	require.NoError(t, b.propose(ctx, CmdPutObjectMeta, PutObjectMetaCmd{
		Bucket: bkt, Key: key, VersionID: preS1Vid, ETag: "etag-preS1",
		Size: 11, ContentType: "text/plain", PreserveLatest: true,
	}))

	// Post-S1 version: normal versioned PUT → writes a per-version blob.
	_ = putVersioned(t, b, ctx, bkt, key, "content-postS1")

	// The pre-S1 version (FSM-only, no blob) must still resolve, not 404.
	hv, err := b.HeadObjectVersion(context.Background(), bkt, key, preS1Vid)
	require.NoError(t, err, "pre-S1 FSM-only version must resolve (RED: 404 from per-version-miss predicate)")
	require.Equal(t, preS1Vid, hv.VersionID)
	require.Equal(t, "etag-preS1", hv.ETag)
}

// TestS2a_EpicA_ReadDeleteEndToEnd is the Epic A core: versioned PUT v1+v2,
// HEAD=v2; DELETE v2 → HEAD=v1, GET ?versionId=v2 → 404, GET(latest)=v1 body;
// then DELETE v1 → HEAD=404.
func TestS2a_EpicA_ReadDeleteEndToEnd(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	const bkt, key = "vbkt", "obj"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))

	vid1 := putVersioned(t, b, ctx, bkt, key, "content-v1")
	vid2 := putVersioned(t, b, ctx, bkt, key, "content-v2")

	head, err := b.HeadObject(ctx, bkt, key)
	require.NoError(t, err)
	require.Equal(t, vid2, head.VersionID)

	require.NoError(t, b.DeleteObjectVersion(bkt, key, vid2))

	head, err = b.HeadObject(ctx, bkt, key)
	require.NoError(t, err)
	require.Equal(t, vid1, head.VersionID, "HEAD re-derives v1 after deleting v2")

	_, _, gverr := b.GetObjectVersion(context.Background(), bkt, key, vid2)
	require.ErrorIs(t, gverr, storage.ErrObjectNotFound)

	rc, _, err := b.GetObject(ctx, bkt, key)
	require.NoError(t, err)
	got, _ := io.ReadAll(rc)
	rc.Close()
	require.Equal(t, []byte("content-v1"), got, "GET latest returns v1 body")

	// NOTE (spec Risks/notes — documented S2a limitation, NOT asserted here):
	// deleting the LAST remaining version drops the key's per-version blobs to
	// zero, so reads fall back to the legacy latest-only quorum-meta blob, which
	// S2a does NOT maintain on hard-delete-of-latest (only PUT/soft-delete
	// dual-write it; S2b switches LIST/latest off it entirely). So after deleting
	// every version, HEAD/GET-by-id can still resolve the stale latest-only blob.
	// That is out of S2a scope (and not a regression — pre-S2a it was wrong too).
}

// TestS2a_GenerationUnion_DeriveAndDelete proves derive-latest unions across
// generations (v1 on group g1/node A, v2 on group g2/node B) — NOT a probeRead
// short-circuit — and that hard-deleting v2 re-derives v1.
func TestS2a_GenerationUnion_DeriveAndDelete(t *testing.T) {
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
	trA.RegisterBufferedRoute(transport.RouteShardRPC, svcA.NativeRPCHandler())
	trB.RegisterBufferedRoute(transport.RouteShardRPC, svcB.NativeRPCHandler())

	const bkt, key = "bkt", "k"
	const vid1 = "019ed400-0000-7000-8000-000000000001"
	const vid2 = "019ed400-0000-7000-8000-000000000002"
	write := func(svc *ShardService, vid, node string) {
		t.Helper()
		blob, err := EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{Bucket: bkt, Key: key, VersionID: vid, ETag: "e-" + vid, NodeIDs: []string{node}, ECData: 1})
		require.NoError(t, err)
		require.NoError(t, svc.writeQuorumMetaVersionLocal(bkt, filepath.Join(key, vid), blob, 0))
	}
	write(svcA, vid1, trA.LocalAddr())
	write(svcB, vid2, trB.LocalAddr())

	backendA := &DistributedBackend{
		selfAddr: trA.LocalAddr(),
		shardSvc: svcA,
		shardGroup: &fakeShardGroupSource{groups: map[string]ShardGroupEntry{
			"g1": {ID: "g1", PeerIDs: []string{trA.LocalAddr()}},
			"g2": {ID: "g2", PeerIDs: []string{trB.LocalAddr()}},
		}},
	}

	cmds, err := backendA.readQuorumMetaVersions(bkt, key)
	require.NoError(t, err)
	latest, live := deriveLatestVersion(cmds)
	require.True(t, live)
	require.Equal(t, vid2, latest.VersionID, "union across generations derives v2 (not probeRead short-circuit)")

	// Hard-delete v2 on its placement node (group g2) via the fan-out, then re-derive.
	v2cmd, ok, _ := backendA.readQuorumMetaVersion(bkt, key, vid2)
	require.True(t, ok)
	require.NoError(t, backendA.deleteQuorumMetaVersionQuorum(ctx, bkt, key, vid2, v2cmd.NodeIDs))

	cmds, err = backendA.readQuorumMetaVersions(bkt, key)
	require.NoError(t, err)
	latest, live = deriveLatestVersion(cmds)
	require.True(t, live)
	require.Equal(t, vid1, latest.VersionID, "after deleting v2, union re-derives v1")
}

// TestDeleteQuorumMetaVersionQuorum_FailClosed proves the fan-out is fail-closed:
// a placement node whose delete fails (unreachable transport) surfaces an error,
// unlike the swallow-all deleteQuorumMetaQuorum.
func TestDeleteQuorumMetaVersionQuorum_FailClosed(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	// "deadnode" is non-self; the single-node shardSvc has a nil transport, so the
	// DeleteQuorumMetaVersion RPC to it fails → the fan-out must return that error.
	err := b.deleteQuorumMetaVersionQuorum(ctx, "bkt", "k", "v1", []string{"deadnode"})
	require.Error(t, err, "fail-closed: an unreachable placement node must surface an error")
}
