package cluster

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeTestShard(t *testing.T, root, bucket, key string) {
	t.Helper()
	dir := filepath.Join(root, bucket, key)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "shard_0"), []byte("data"), 0o644))
}

func TestLocalObjectPicker_PicksFirstLocalShard(t *testing.T) {
	root := t.TempDir()
	writeTestShard(t, root, "bucket1", "key1")

	picker := NewLocalObjectPicker(root)
	bucket, key, versionID, ok := picker.PickObjectOnSrcNode("self")
	require.True(t, ok)
	assert.Equal(t, "bucket1", bucket)
	assert.Equal(t, "key1", key)
	assert.Equal(t, "", versionID) // no versioning in shard paths
}

func TestLocalObjectPicker_EmptyDir_ReturnsFalse(t *testing.T) {
	root := t.TempDir()
	picker := NewLocalObjectPicker(root)
	_, _, _, ok := picker.PickObjectOnSrcNode("self")
	assert.False(t, ok)
}

func TestLocalObjectPicker_OnlyPicksLocalShards(t *testing.T) {
	// Only objects with shard_0 on the local filesystem are returned.
	// Objects only in BadgerDB metadata (cluster-wide) are not returned.
	root := t.TempDir()
	writeTestShard(t, root, "mybucket", "localkey")
	// "remotekey" has no shard files here (only on remote node's disk)

	picker := NewLocalObjectPicker(root)
	bucket, key, _, ok := picker.PickObjectOnSrcNode("self")
	require.True(t, ok)
	assert.Equal(t, "mybucket", bucket)
	assert.Equal(t, "localkey", key)
}

func TestLocalObjectPicker_NodeArgIgnored(t *testing.T) {
	// SrcNode is always the leader itself; nodeID arg exists for interface compatibility.
	root := t.TempDir()
	writeTestShard(t, root, "bkt", "k")

	picker := NewLocalObjectPicker(root)
	_, _, _, ok := picker.PickObjectOnSrcNode("any-node-id")
	assert.True(t, ok)
}

func TestBalancerProposer_OkFalse_SkipsMigrationProposal(t *testing.T) {
	// When ObjectPicker returns ok=false, proposeMigration should not send a proposal.
	store := NewNodeStatsStore(0) // no TTL
	store.Set(NodeStats{NodeID: "node-a", DiskUsedPct: 80.0})
	store.Set(NodeStats{NodeID: "node-b", DiskUsedPct: 40.0})

	node := &mockRaftNode{state: 2, nodeID: "node-a", peerIDs: []string{"node-b"}}
	cfg := testBalancerConfig()

	p := NewBalancerProposer("node-a", store, node, cfg)
	// Set an empty picker that always returns ok=false.
	p.SetObjectPicker(&mockObjectPicker{ok: false})

	p.tickOnce(context.Background())
	assert.Empty(t, node.proposed, "no proposal when picker returns ok=false")
}

// mockObjectPicker is a test double for ObjectPicker.
type mockObjectPicker struct {
	bucket, key, versionID string
	ok                     bool
}

func (m *mockObjectPicker) PickObjectOnSrcNode(_ string) (string, string, string, bool) {
	return m.bucket, m.key, m.versionID, m.ok
}
