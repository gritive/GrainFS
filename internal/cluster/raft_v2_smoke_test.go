package cluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft"
)

// noopRV and noopAE are transport stubs that always fail (no peers in smoke tests).
var noopRV = func(peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	return nil, fmt.Errorf("no transport")
}
var noopAE = func(peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	return nil, fmt.Errorf("no transport")
}

// TestRaftV2Smoke_DefaultClusterIsV2 verifies that newRaftNode returns the v2
// adapter. As of M5 PR 29 v2 is the only path (the GRAINFS_RAFT_V2 flag was
// removed). PR 30 deletes the v1 raft package outright.
func TestRaftV2Smoke_DefaultClusterIsV2(t *testing.T) {
	rcfg := raft.DefaultConfig("node-1", nil)
	node, _, err := newRaftNode(rcfg, "")
	require.NoError(t, err)

	_, isV1 := node.(*raft.Node)
	assert.False(t, isV1, "expected v2 adapter (v1 path was removed in PR 29)")
	_, isV2 := node.(*raftNodeAdapter)
	assert.True(t, isV2, "expected *raftNodeAdapter adapter")

	node.SetTransport(noopRV, noopAE)
	node.Start()
	defer node.Close()

	assert.Equal(t, "node-1", node.ID())
}

// TestRaftV2Smoke_BootstrapProposeRoundtrip verifies the basic single-node v2
// path: bootstrap → elect self → ProposeWait commits.
func TestRaftV2Smoke_BootstrapProposeRoundtrip(t *testing.T) {
	rcfg := raft.DefaultConfig("node-1", nil)
	node, _, err := newRaftNode(rcfg, "")
	require.NoError(t, err)

	node.SetTransport(noopRV, noopAE)
	node.Start()
	defer node.Close()

	err = node.Bootstrap()
	if err != nil {
		require.ErrorContains(t, err, "already bootstrapped",
			"unexpected bootstrap error: %v", err)
	}

	// Wait for leader election (single-node converges quickly).
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for !node.IsLeader() {
		select {
		case <-ctx.Done():
			t.Fatal("timed out waiting for v2 node to become leader")
		case <-time.After(10 * time.Millisecond):
		}
	}

	// ProposeWait on the single-node leader.
	proposeCtx, proposeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer proposeCancel()
	idx, err := node.ProposeWait(proposeCtx, []byte("smoke"))
	require.NoError(t, err)
	assert.Greater(t, idx, uint64(0), "expected non-zero committed index from ProposeWait")
}
