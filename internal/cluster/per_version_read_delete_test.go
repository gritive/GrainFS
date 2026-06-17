package cluster

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/gritive/GrainFS/internal/transport"
	"github.com/stretchr/testify/require"
)

// TestReadQuorumMetaVersionsLocal_ListsKeyVersions proves the local lister
// enumerates every per-version blob under .quorum_meta_versions/{bucket}/{key}/.
func TestReadQuorumMetaVersionsLocal_ListsKeyVersions(t *testing.T) {
	b := newTestDistributedBackend(t)
	// Two version blobs for one key, written via the S1 local primitive.
	for _, vid := range []string{"v1", "v2"} {
		blob, err := EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{Bucket: "bkt", Key: "a/b/c.txt", VersionID: vid, ETag: "e-" + vid})
		require.NoError(t, err)
		require.NoError(t, b.shardSvc.writeQuorumMetaVersionLocal("bkt", filepath.Join("a/b/c.txt", vid), blob))
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
		require.NoError(t, svcB.writeQuorumMetaVersionLocal("bkt", filepath.Join("k", vid), blob))
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
		require.NoError(t, svc.writeQuorumMetaVersionLocal("bkt", filepath.Join("k", vid), blob))
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
