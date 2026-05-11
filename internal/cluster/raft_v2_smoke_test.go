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

// TestRaftV2Smoke_DefaultClusterIsV1 verifies that with no flag set, the
// cluster package still selects a v1 node. M5 PR 28 default-on covers
// serveruntime only; cluster is held until PR 28b (group-raft mux wired
// through the v2 RPC bridge — see raftV2DefaultOnPkgs in raftflag.go).
func TestRaftV2Smoke_DefaultClusterIsV1(t *testing.T) {
	t.Setenv("GRAINFS_RAFT_V2", "")
	resetRaftV2FlagForTest()
	t.Cleanup(resetRaftV2FlagForTest)

	rcfg := raft.DefaultConfig("node-1", nil)
	node, _, err := newRaftNode(rcfg, nil, "")
	require.NoError(t, err)

	v1, ok := node.(*raft.Node)
	assert.True(t, ok, "expected *raft.Node (v1) for cluster when flag is unset")
	assert.NotNil(t, v1)

	node.SetTransport(noopRV, noopAE)
	node.Start()
	defer node.Close()

	assert.Equal(t, "node-1", node.ID())
}

// TestRaftV2Smoke_DefaultServeruntimeIsV2 documents the M5 PR 28 phased flip:
// serveruntime is now v2 by default while cluster remains v1.
func TestRaftV2Smoke_DefaultServeruntimeIsV2(t *testing.T) {
	t.Setenv("GRAINFS_RAFT_V2", "")
	resetRaftV2FlagForTest()
	t.Cleanup(resetRaftV2FlagForTest)

	assert.True(t, IsV2Enabled("serveruntime"),
		"expected serveruntime to default to v2 when GRAINFS_RAFT_V2 is unset")
	assert.False(t, IsV2Enabled("cluster"),
		"expected cluster to stay v1 when GRAINFS_RAFT_V2 is unset (held for PR 28b)")
}

// TestRaftV2Smoke_OffEscapeHatchDisablesV2 verifies the operator escape hatch:
// GRAINFS_RAFT_V2=off disables v2 for every package (reverts serveruntime back
// to v1; cluster stays v1).
func TestRaftV2Smoke_OffEscapeHatchDisablesV2(t *testing.T) {
	t.Setenv("GRAINFS_RAFT_V2", "off")
	resetRaftV2FlagForTest()
	t.Cleanup(resetRaftV2FlagForTest)

	assert.False(t, IsV2Enabled("serveruntime"),
		"GRAINFS_RAFT_V2=off must disable v2 for serveruntime")
	assert.False(t, IsV2Enabled("cluster"),
		"GRAINFS_RAFT_V2=off must disable v2 for cluster")

	rcfg := raft.DefaultConfig("node-1", nil)
	node, _, err := newRaftNode(rcfg, nil, "")
	require.NoError(t, err)

	v1, ok := node.(*raft.Node)
	assert.True(t, ok, "expected *raft.Node (v1) when GRAINFS_RAFT_V2=off")
	assert.NotNil(t, v1)

	node.SetTransport(noopRV, noopAE)
	node.Start()
	defer node.Close()
}

// TestRaftV2Smoke_ClusterFlagSelectsV2 verifies that GRAINFS_RAFT_V2=cluster
// selects the v2 adapter and that Bootstrap+ProposeWait works.
func TestRaftV2Smoke_ClusterFlagSelectsV2(t *testing.T) {
	t.Setenv("GRAINFS_RAFT_V2", "cluster")
	resetRaftV2FlagForTest()
	t.Cleanup(resetRaftV2FlagForTest)

	rcfg := raft.DefaultConfig("node-1", nil)
	node, _, err := newRaftNode(rcfg, nil, "")
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
	node, _, err := newRaftNode(rcfg, nil, "")
	require.NoError(t, err)

	// Must still be v1.
	_, isV1 := node.(*raft.Node)
	assert.True(t, isV1, "GRAINFS_RAFT_V2=serveruntime must not affect cluster package")

	node.SetTransport(noopRV, noopAE)
	node.Start()
	defer node.Close()

	os.Remove(node.ID()) // cleanup any state files
}

// TestRaftV2Smoke_ParseFlag verifies the flag parser handles all documented
// cases. As of M5 PR 28 an empty env defaults to serveruntime=v2 (cluster
// stays v1 until PR 28b); "off" is the escape hatch that disables v2.
func TestRaftV2Smoke_ParseFlag(t *testing.T) {
	cases := []struct {
		name    string
		env     string
		cluster bool
		srv     bool
	}{
		{"empty_defaults_to_serveruntime_v2", "", false, true},
		{"off_disables_all", "off", false, false},
		{"cluster_only", "cluster", true, false},
		{"serveruntime_only", "serveruntime", false, true},
		{"both_named", "cluster,serveruntime", true, true},
		{"all_alias", "all", true, true},
		{"unknown_ignored", "unknown", false, false},
		{"unknown_plus_cluster", "junk,cluster", true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ParseRaftV2Flag(tc.env)
			assert.Equal(t, tc.cluster, got["cluster"], "cluster")
			assert.Equal(t, tc.srv, got["serveruntime"], "serveruntime")
		})
	}
}
