package cluster

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft"
)

// TestNewRaftNode_V2DurableStoresAtV2Subdir verifies that a non-empty v2
// store dir causes newRaftNode to open a Badger DB at <dir>/raft-v2/. Closes
// the PR 22 deferral: v2's BadgerLogStore was implemented but never wired
// through the factory.
func TestNewRaftNode_V2DurableStoresAtV2Subdir(t *testing.T) {
	tmp := t.TempDir()
	rcfg := raft.DefaultConfig("node-A", nil)

	node, closeFn, err := newRaftNode(rcfg, tmp)
	require.NoError(t, err)
	require.NotNil(t, node)
	require.NotNil(t, closeFn, "v2 path with non-empty store dir must return a closeFn for the Badger DB")

	node.SetTransport(noopRV, noopAE)
	node.Start()
	t.Cleanup(func() {
		node.Close()
		_ = closeFn()
	})

	// Verify the sub-directory was created.
	subDir := filepath.Join(tmp, raftV2StoreSubdir)
	info, err := os.Stat(subDir)
	require.NoError(t, err, "v2 store sub-directory must exist")
	require.True(t, info.IsDir(), "v2 store path must be a directory")

	// Verify it's the v2 adapter, not v1 (PR 30 deletes the v1 package).
	_, isV1 := any(node).(*raft.Node)
	require.False(t, isV1, "expected v2 adapter (v1 path was removed in PR 29)")
}

// TestNewRaftNode_V2DurableStoresSurviveRestart verifies the closed PR 22
// deferral: when a v2 node bootstraps, proposes an entry, then the node
// + DB are closed and reopened against the same directory, the committed
// state persists. Exercises the full v2 LogStore wire-up:
// open → Append → fsync → close → reopen → read.
func TestNewRaftNode_V2DurableStoresSurviveRestart(t *testing.T) {
	tmp := t.TempDir()
	rcfg := raft.DefaultConfig("node-A", nil)

	// First boot: bootstrap a single-node cluster, propose an entry, wait
	// for it to commit, then shut down.
	var (
		firstIdx  uint64
		firstTerm uint64
	)
	func() {
		node, closeFn, err := newRaftNode(rcfg, tmp)
		require.NoError(t, err)
		defer func() {
			node.Close()
			_ = closeFn()
		}()

		node.SetTransport(noopRV, noopAE)
		node.Start()
		require.NoError(t, node.Bootstrap())

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for !node.IsLeader() {
			select {
			case <-ctx.Done():
				t.Fatal("timed out waiting for v2 node to become leader")
			case <-time.After(10 * time.Millisecond):
			}
		}

		propCtx, propCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer propCancel()
		idx, err := node.ProposeWait(propCtx, []byte("durable-entry"))
		require.NoError(t, err)
		require.Greater(t, idx, uint64(0))
		firstIdx = idx
		firstTerm = node.Term()
	}()

	// Second boot: reopen against the same directory. The persisted
	// HardState restores Term (Raft §5.4.1 safety); the LogStore restores
	// the log so CommittedIndex picks up where we left off after the actor
	// settles.
	node2, closeFn2, err := newRaftNode(rcfg, tmp)
	require.NoError(t, err)
	t.Cleanup(func() {
		node2.Close()
		_ = closeFn2()
	})
	node2.SetTransport(noopRV, noopAE)
	node2.Start()

	// Term must be ≥ firstTerm: the restart inherits the persisted term
	// (single-node clusters re-promote to leader, often bumping the term).
	require.GreaterOrEqual(t, node2.Term(), firstTerm,
		"v2 StableStore must restore Term across restart (Raft §5.4.1)")
	// The proposed entry's index is durable in the LogStore — confirm by
	// waiting for the actor to re-elect itself and seeing that the next
	// Propose lands strictly after firstIdx.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// v2 bootstrapped flag is in-memory; restart sees a fresh false → nil.
	require.NoError(t, node2.Bootstrap())
	for !node2.IsLeader() {
		select {
		case <-ctx.Done():
			t.Fatal("timed out waiting for v2 node to re-become leader")
		case <-time.After(10 * time.Millisecond):
		}
	}
	propCtx, propCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer propCancel()
	idx2, err := node2.ProposeWait(propCtx, []byte("post-restart-entry"))
	require.NoError(t, err)
	require.Greater(t, idx2, firstIdx,
		"post-restart Propose must land strictly after the durable firstIdx (%d > %d)", idx2, firstIdx)
}

// TestNewRaftNode_V2EmptyDirFallsBackToMemoryStore verifies the
// backward-compat behaviour: empty v2StoreDir keeps the v2 path on memory
// stores (matches PR 22 behaviour for tests that never pass a directory).
func TestNewRaftNode_V2EmptyDirFallsBackToMemoryStore(t *testing.T) {
	rcfg := raft.DefaultConfig("node-A", nil)
	node, closeFn, err := newRaftNode(rcfg, "")
	require.NoError(t, err)
	require.NotNil(t, node)
	require.Nil(t, closeFn, "memory store path must not allocate a closeFn")
	node.SetTransport(noopRV, noopAE)
	node.Start()
	t.Cleanup(func() { node.Close() })
}
