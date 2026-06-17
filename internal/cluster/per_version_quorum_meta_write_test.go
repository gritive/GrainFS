package cluster

import (
	"os"
	"path/filepath"
	"testing"

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
