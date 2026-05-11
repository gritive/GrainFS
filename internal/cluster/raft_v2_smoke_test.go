package cluster

import (
	"context"
	"fmt"
	"os"
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

// TestRaftV2Smoke_DefaultIsV1 verifies that with no flag set, newRaftNode
// returns a v1 node, and that GroupBackend.RaftNode() type-asserts correctly.
func TestRaftV2Smoke_DefaultIsV1(t *testing.T) {
	t.Setenv("GRAINFS_RAFT_V2", "")
	resetRaftV2FlagForTest()
	t.Cleanup(resetRaftV2FlagForTest)

	rcfg := raft.DefaultConfig("node-1", nil)
	node, err := newRaftNode(rcfg, nil)
	require.NoError(t, err)

	// Should be *raft.Node (v1).
	v1, ok := node.(*raft.Node)
	assert.True(t, ok, "expected *raft.Node (v1) when flag is unset")
	assert.NotNil(t, v1)

	node.SetTransport(noopRV, noopAE)
	node.Start()
	defer node.Close()

	assert.Equal(t, "node-1", node.ID())
}

// TestRaftV2Smoke_ClusterFlagSelectsV2 verifies that GRAINFS_RAFT_V2=cluster
// selects the v2 adapter and that Bootstrap+ProposeWait works.
func TestRaftV2Smoke_ClusterFlagSelectsV2(t *testing.T) {
	t.Setenv("GRAINFS_RAFT_V2", "cluster")
	resetRaftV2FlagForTest()
	t.Cleanup(resetRaftV2FlagForTest)

	rcfg := raft.DefaultConfig("node-1", nil)
	node, err := newRaftNode(rcfg, nil)
	require.NoError(t, err)

	// Should NOT be *raft.Node — it's the v2 adapter.
	_, isV1 := node.(*raft.Node)
	assert.False(t, isV1, "expected v2 adapter when GRAINFS_RAFT_V2=cluster")

	// Should be *raftV2Node.
	_, isV2 := node.(*raftV2Node)
	assert.True(t, isV2, "expected *raftV2Node adapter")

	node.SetTransport(noopRV, noopAE)
	node.Start()
	defer node.Close()

	assert.Equal(t, "node-1", node.ID())

	// Single-node cluster: bootstrap and elect self as leader.
	err = node.Bootstrap()
	if err != nil {
		// Already bootstrapped is fine in subsequent runs.
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

// TestRaftV2Smoke_OtherPkgFlagDoesNotAffectCluster verifies that setting
// GRAINFS_RAFT_V2=serveruntime does NOT enable v2 for the cluster package.
func TestRaftV2Smoke_OtherPkgFlagDoesNotAffectCluster(t *testing.T) {
	t.Setenv("GRAINFS_RAFT_V2", "serveruntime")
	resetRaftV2FlagForTest()
	t.Cleanup(resetRaftV2FlagForTest)

	rcfg := raft.DefaultConfig("node-1", nil)
	node, err := newRaftNode(rcfg, nil)
	require.NoError(t, err)

	// Must still be v1.
	_, isV1 := node.(*raft.Node)
	assert.True(t, isV1, "GRAINFS_RAFT_V2=serveruntime must not affect cluster package")

	node.SetTransport(noopRV, noopAE)
	node.Start()
	defer node.Close()

	os.Remove(node.ID()) // cleanup any state files
}

// TestRaftV2Smoke_ParseFlag verifies the flag parser handles all documented cases.
func TestRaftV2Smoke_ParseFlag(t *testing.T) {
	cases := []struct {
		env     string
		cluster bool
		srv     bool
	}{
		{"", false, false},
		{"cluster", true, false},
		{"serveruntime", false, true},
		{"cluster,serveruntime", true, true},
		{"all", true, true},
		{"unknown", false, false},
		{"cluster,unknown", true, false},
	}
	for _, tc := range cases {
		t.Run(tc.env, func(t *testing.T) {
			got := ParseRaftV2Flag(tc.env)
			assert.Equal(t, tc.cluster, got["cluster"], "cluster")
			assert.Equal(t, tc.srv, got["serveruntime"], "serveruntime")
		})
	}
}
