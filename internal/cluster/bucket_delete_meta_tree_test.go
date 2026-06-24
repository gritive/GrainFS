package cluster

import (
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
