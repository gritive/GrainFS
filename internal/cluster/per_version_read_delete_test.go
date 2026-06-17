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
