package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newSoloNode creates a durable single-voter raft node under dir and returns
// it plus a closeStore func to call after node.Close() on cleanup.
func newSoloNode(t *testing.T, dir string) (RaftNode, func() error) {
	t.Helper()
	rcfg := raft.DefaultConfig("n1", nil)
	node, closeStore, err := newRaftNode(rcfg, dir)
	require.NoError(t, err)
	return node, closeStore
}

// startSoloIndexGroup builds a KEK-wired MetaFSM, creates an indexGroup
// backed by the given solo node (nil forward = leader-local), starts it,
// and waits for the node to elect itself leader.
func startSoloIndexGroup(t *testing.T, node RaftNode) *indexGroup {
	t.Helper()
	fsm := NewMetaFSM()
	wireTestKEK(t, fsm)
	ig := newIndexGroup(node, fsm, nil)
	require.NoError(t, ig.Start(context.Background()))
	require.Eventually(t, node.IsLeader, 5*time.Second, 20*time.Millisecond,
		"solo node should elect itself leader")
	return ig
}

// TestIndexGroup_SingleNode_PutGetDeleteRoundTrip verifies propose + read-your-write
// + delete on a real solo raft node.
func TestIndexGroup_SingleNode_PutGetDeleteRoundTrip(t *testing.T) {
	dir := t.TempDir()
	node, closeStore := newSoloNode(t, dir)
	ig := startSoloIndexGroup(t, node)
	t.Cleanup(func() { ig.Close() })
	t.Cleanup(func() { _ = closeStore() })

	ctx := context.Background()

	// Put v1.
	err := ig.ProposeObjectIndex(ctx, ObjectIndexEntry{
		Bucket:           "b",
		Key:              "k",
		VersionID:        "v1",
		PlacementGroupID: "g0",
		Size:             10,
		ModTime:          100,
	}, false)
	require.NoError(t, err)

	// Read-your-write: v1 must be visible immediately.
	got, ok := ig.ObjectIndexLatest("b", "k")
	require.True(t, ok, "expected entry after put")
	assert.Equal(t, "v1", got.VersionID)
	assert.Equal(t, int64(10), got.Size)

	// Delete v1.
	err = ig.ProposeDeleteObjectIndex(ctx, "b", "k", "v1")
	require.NoError(t, err)

	// After delete, the entry must be gone.
	_, ok = ig.ObjectIndexLatest("b", "k")
	assert.False(t, ok, "entry should be gone after delete")
}

// TestIndexGroup_SingleNode_SnapshotRestoreOnRestart verifies that snapshot()
// persists state and a freshly-opened node + indexGroup restores from it.
func TestIndexGroup_SingleNode_SnapshotRestoreOnRestart(t *testing.T) {
	dir := t.TempDir()

	// ── Phase 1: build state and take snapshot ──────────────────────────────
	node, closeStore := newSoloNode(t, dir)
	ig := startSoloIndexGroup(t, node)

	ctx := context.Background()

	for _, v := range []string{"v1", "v2", "v3"} {
		err := ig.ProposeObjectIndex(ctx, ObjectIndexEntry{
			Bucket:           "b",
			Key:              "k",
			VersionID:        v,
			PlacementGroupID: "g0",
			Size:             1,
			ModTime:          1,
		}, false)
		require.NoError(t, err, "propose %s", v)
	}

	data, idx, err := ig.snapshot()
	require.NoError(t, err)
	assert.Greater(t, idx, uint64(0), "snapshot index must be > 0")
	assert.NotEmpty(t, data, "snapshot data must be non-empty")

	// ── Phase 2: restart ──────────────────────────────────────────────────
	ig.Close()
	require.NoError(t, closeStore())

	node2, closeStore2 := newSoloNode(t, dir)
	t.Cleanup(func() { _ = closeStore2() })

	fsm2 := NewMetaFSM()
	wireTestKEK(t, fsm2)
	ig2 := newIndexGroup(node2, fsm2, nil)
	require.NoError(t, ig2.Start(ctx))
	t.Cleanup(func() { ig2.Close() })

	// After Start(), the snapshot is restored. ObjectIndexVersion for v2 must exist.
	got, ok := ig2.ObjectIndexVersion("b", "k", "v2")
	require.True(t, ok, "v2 should be present after snapshot restore")
	assert.Equal(t, "v2", got.VersionID)
}
