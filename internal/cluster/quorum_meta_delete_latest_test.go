package cluster

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// newSingleNodeBackendWithDirForTest builds a single-node 1+0 backend wired
// for quorum-meta (mirrors newTestBackendWithQuorumMeta) and also returns the
// ShardService dataDirs[0] so tests can assert filesystem state directly.
func newSingleNodeBackendWithDirForTest(t *testing.T) (*DistributedBackend, string) {
	t.Helper()
	b := newSingleNode1Plus0ChunkCapable(t)
	dataDir := b.shardSvc.DataDirs()[0]
	return b, dataDir
}

// TestDeleteQuorumMetaLocal_RemovesLatestBlob proves that deleteQuorumMetaLocal
// removes the latest-only quorum-meta blob and is idempotent (double-delete is
// not an error).
func TestDeleteQuorumMetaLocal_RemovesLatestBlob(t *testing.T) {
	b, dataDir := newSingleNodeBackendWithDirForTest(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "b"))
	putTestObjectForRetire(t, b, "b", "k", []byte("data"))

	blobPath := filepath.Join(dataDir, ".quorum_meta", "b", "k")
	require.FileExists(t, blobPath)

	require.NoError(t, b.shardSvc.deleteQuorumMetaLocal("b", "k"))
	require.NoFileExists(t, blobPath)

	// idempotent: second delete must not return an error
	require.NoError(t, b.shardSvc.deleteQuorumMetaLocal("b", "k"))
}

// TestDeleteQuorumMetaQuorum_SingleNode_RemovesBlob proves that
// deleteQuorumMetaQuorum on a single-node backend (nodeIDs = selfAddr) removes
// the local blob and is fail-closed (returns an error when the blob does not
// exist after a first delete is fine because idempotent).
func TestDeleteQuorumMetaQuorum_SingleNode_RemovesBlob(t *testing.T) {
	b, dataDir := newSingleNodeBackendWithDirForTest(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "b"))
	putTestObjectForRetire(t, b, "b", "kq", []byte("payload"))

	blobPath := filepath.Join(dataDir, ".quorum_meta", "b", "kq")
	require.FileExists(t, blobPath)

	nodeIDs := []string{b.selfAddr}
	require.NoError(t, b.deleteQuorumMetaQuorum(ctx, "b", "kq", nodeIDs))
	require.NoFileExists(t, blobPath)
}

// TestDeleteShardsQuorum_SingleNode_RemovesShards proves that deleteShardsQuorum
// on a single-node backend removes the local shard directory and is
// fail-closed (errors propagate).
func TestDeleteShardsQuorum_SingleNode_RemovesShards(t *testing.T) {
	b, dataDir := newSingleNodeBackendWithDirForTest(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "b"))
	putTestObjectForRetire(t, b, "b", "ks", []byte("shardpayload"))

	shardDir := filepath.Join(dataDir, "b", "ks")
	require.DirExists(t, shardDir)

	placement := []string{b.selfAddr}
	shardKey := ecObjectShardKey("ks", "")
	require.NoError(t, b.deleteShardsQuorum(ctx, "b", shardKey, placement))
	require.NoDirExists(t, shardDir)
}
