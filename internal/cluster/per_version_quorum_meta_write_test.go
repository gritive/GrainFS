package cluster

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/gritive/GrainFS/internal/transport"
	"github.com/stretchr/testify/require"
)

// TestWriteQuorumMetaVersionLocal_WritesToSeparateSubtree proves the per-version
// blob lands at .quorum_meta_versions/{bucket}/{key}/{vid} (separate from the
// latest-only .quorum_meta tree) and is byte-identical to the data written.
func TestWriteQuorumMetaVersionLocal_WritesToSeparateSubtree(t *testing.T) {
	b := newTestDistributedBackend(t)
	data := []byte("blob-bytes")

	require.NoError(t, b.shardSvc.writeQuorumMetaVersionLocal("bkt", filepath.Join("a/b/c.txt", "vid-1"), data))

	root := b.shardSvc.dataDirs[0]
	verPath := filepath.Join(root, quorumMetaVersionsSubDir, "bkt", "a/b/c.txt", "vid-1")
	got, err := os.ReadFile(verPath)
	require.NoError(t, err, "per-version blob must exist at %s", verPath)
	require.Equal(t, data, got)

	// The latest-only tree must be untouched (no collision, separate subtree).
	latPath := filepath.Join(root, quorumMetaSubDir, "bkt", "a/b/c.txt")
	_, statErr := os.Stat(latPath)
	require.True(t, os.IsNotExist(statErr), "latest-only tree must not be written by the version primitive")
}

// TestWriteQuorumMetaVersion_RPC proves the per-version write RPC durably writes
// the blob on a remote placement node's separate subtree.
func TestWriteQuorumMetaVersion_RPC(t *testing.T) {
	ctx := context.Background()
	keeper, clusterID := testDEKKeeper(t)

	trA := transport.MustNewHTTPTransport("test-cluster-psk")
	trB := transport.MustNewHTTPTransport("test-cluster-psk")
	require.NoError(t, trA.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, trB.Listen(ctx, "127.0.0.1:0"))
	defer trA.Close()
	defer trB.Close()

	dirB := t.TempDir()
	svcA := NewShardService(t.TempDir(), trA, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))
	svcB := NewShardService(dirB, trB, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))
	trB.RegisterBufferedRoute(transport.RouteShardRPC, svcB.NativeRPCHandler())

	data := []byte("ver-blob")
	require.NoError(t, svcA.WriteQuorumMetaVersion(ctx, trB.LocalAddr(), "bkt", filepath.Join("k", "vid-1"), data))

	got, err := os.ReadFile(filepath.Join(dirB, "shards", quorumMetaVersionsSubDir, "bkt", "k", "vid-1"))
	require.NoError(t, err, "remote node must have written the per-version blob")
	require.Equal(t, data, got)
}
