package cluster

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestForceDeleteBucketRemovesQuorumMetaTrees proves a versioned force-delete
// physically removes the per-bucket off-raft quorum-meta blob trees
// (.quorum_meta/{bucket} and .quorum_meta_versions/{bucket}). os.RemoveAll on
// bucketDir only clears {root}/data/{bucket}; without this the hard-delete
// tombstone blobs purgePerVersionBlobs writes persist as inert residue.
func TestForceDeleteBucketRemovesQuorumMetaTrees(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := t.Context()
	require.NoError(t, b.CreateBucket(ctx, "vb"))
	require.NoError(t, b.SetBucketVersioning("vb", "Enabled"))
	putTestObjectForRetire(t, b, "vb", "k", []byte("a"))

	dataDir := b.shardSvc.DataDirs()[0]
	versTree := filepath.Join(dataDir, ".quorum_meta_versions", "vb")
	require.DirExists(t, versTree)

	require.NoError(t, b.ForceDeleteBucket(ctx, "vb"))

	require.NoDirExists(t, versTree, "force-delete must physically remove the per-bucket .quorum_meta_versions tree")
	require.NoDirExists(t, filepath.Join(dataDir, ".quorum_meta", "vb"), "force-delete must physically remove the per-bucket .quorum_meta tree")
}

func TestShardServiceRemoveBucketPhysicalTreesRPCRemovesBucketAndMeta(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	svc := b.shardSvc
	shardRoot := svc.DataDirs()[0]
	dataRoot := filepath.Dir(shardRoot)

	require.NoError(t, os.MkdirAll(filepath.Join(dataRoot, "b", "legacy"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dataRoot, "b", "legacy", "object"), []byte("x"), 0o644))
	require.NoError(t, os.MkdirAll(filepath.Join(shardRoot, ".quorum_meta", "b"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(shardRoot, ".quorum_meta", "b", "k"), []byte("q"), 0o644))
	require.NoError(t, os.MkdirAll(filepath.Join(shardRoot, ".quorum_meta_versions", "b", "k"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(shardRoot, ".quorum_meta_versions", "b", "k", "v1"), []byte("qv"), 0o644))

	envb := buildShardEnvelope("RemoveBucketPhysicalTrees", "b", "", 0, nil)
	defer func() { envb.Reset(); shardBuilderPool.Put(envb) }()
	resp := svc.handleRPC(envb.FinishedBytes())
	rpcType, data, err := unmarshalEnvelope(resp)
	require.NoError(t, err)
	require.Equalf(t, "OK", rpcType, "response: %s", string(data))

	require.NoDirExists(t, filepath.Join(dataRoot, "b"))
	require.NoDirExists(t, filepath.Join(shardRoot, ".quorum_meta", "b"))
	require.NoDirExists(t, filepath.Join(shardRoot, ".quorum_meta_versions", "b"))
}
