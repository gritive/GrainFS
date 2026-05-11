package cluster

import (
	"context"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft"
)

// TestDistributedBackend_TriggerRaftSnapshot_V2 verifies the v2 snapshot trigger
// surface (M5 PR 27 Part B): with GRAINFS_RAFT_V2=cluster the admin
// TriggerRaftSnapshot / RaftSnapshotStatus paths must work even though the v1
// SnapshotManager is bypassed in v2 mode.
//
// We bootstrap a single-voter v2 cluster (no peer transport needed for snapshot
// trigger), apply one mutation via CreateBucket, then trigger a snapshot and
// assert the status surfaces it.
func TestDistributedBackend_TriggerRaftSnapshot_V2(t *testing.T) {
	t.Setenv("GRAINFS_RAFT_V2", "cluster")
	resetRaftV2FlagForTest()
	t.Cleanup(resetRaftV2FlagForTest)

	dir := t.TempDir()
	db, err := badger.Open(badger.DefaultOptions(dir + "/meta").WithLogger(nil))
	require.NoError(t, err)

	rcfg := raft.DefaultConfig("v2-leader", nil)
	node, closeFn, err := newRaftNode(rcfg, nil, dir)
	require.NoError(t, err)
	require.NotNil(t, closeFn, "v2 path expected: closeFn must be non-nil")

	node.SetTransport(noopRV, noopAE)
	node.Start()

	t.Cleanup(func() {
		node.Close()
		_ = closeFn()
		db.Close()
	})

	require.NoError(t, node.Bootstrap())
	require.Eventually(t, node.IsLeader, 3*time.Second, 20*time.Millisecond, "v2 node must elect itself")

	backend, err := NewDistributedBackend(dir, db, node)
	require.NoError(t, err)

	// Deliberately do NOT call SetSnapshotManager — that's the v2 path:
	// bootSnapshotAndApplyLoop skips it for raftv2 nodes.
	stopApply := make(chan struct{})
	go backend.RunApplyLoop(stopApply)
	t.Cleanup(func() { close(stopApply) })

	require.NoError(t, backend.CreateBucket(context.Background(), "v2-trigger"))
	require.Eventually(t, func() bool {
		return backend.lastApplied.Load() > 0
	}, 3*time.Second, 10*time.Millisecond, "FSM must apply at least one entry")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := backend.TriggerRaftSnapshot(ctx)
	require.NoError(t, err)
	require.Equal(t, backend.lastApplied.Load(), result.Index)
	require.Greater(t, result.SizeBytes, 0)

	status, err := backend.RaftSnapshotStatus()
	require.NoError(t, err)
	require.True(t, status.Available, "v2 status must report Available after CreateSnapshot")
	require.Equal(t, result.Index, status.Index)
	require.Equal(t, result.SizeBytes, status.SizeBytes)
}
