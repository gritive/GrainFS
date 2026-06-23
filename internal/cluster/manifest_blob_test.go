package cluster

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestManifestBlob_RoundTripSiblingRoot verifies that the manifest blob
// primitive stores and retrieves an upload manifest at the sibling-root
// .qmeta_mpu/{bucket}/{uploadID} path, and that the file is invisible to the
// object-store walker (ScanQuorumMetaBucket).
func TestManifestBlob_RoundTripSiblingRoot(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	m := clusterMultipartMeta{Bucket: "bkt", Key: "k", ContentType: "text/plain", CreatedAt: 100}
	require.NoError(t, b.writeManifestBlob(ctx, m, "up-1", b.selfNodeIDs())) // single node placement
	got, ok, err := b.readManifestBlob("bkt", "up-1")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "k", got.Key)
	require.DirExists(t, filepath.Join(b.dataDir0(), ".qmeta_mpu", "bkt")) // add b.dataDir0() accessor if absent
	objs, _ := b.shardSvc.ScanQuorumMetaBucket("bkt", "")                  // ShardService, not backend
	require.Empty(t, objs, "manifest blob invisible to the object-store walker")
	require.NoError(t, b.deleteManifestBlob("bkt", "up-1"))
	_, ok, _ = b.readManifestBlob("bkt", "up-1")
	require.False(t, ok)
}
