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

// TestRaftV2Smoke_DefaultClusterIsV2 verifies that with no flag set, the
// cluster package now selects a v2 node. M5 PR 28b adds "cluster" to
// raftV2DefaultOnPkgs once the per-group QUIC mux dispatch is wired through
// RegisterV2 (see internal/raft/group_transport_quic.go).
func TestRaftV2Smoke_DefaultClusterIsV2(t *testing.T) {
	t.Setenv("GRAINFS_RAFT_V2", "")
	resetRaftV2FlagForTest()
	t.Cleanup(resetRaftV2FlagForTest)

	rcfg := raft.DefaultConfig("node-1", nil)
	node, _, err := newRaftNode(rcfg, nil, "")
	require.NoError(t, err)

	_, isV1 := node.(*raft.Node)
	assert.False(t, isV1, "expected v2 adapter for cluster when flag is unset (PR 28b)")
	_, isV2 := node.(*raftV2Node)
	assert.True(t, isV2, "expected *raftV2Node adapter for cluster when flag is unset")

	node.SetTransport(noopRV, noopAE)
	node.Start()
	defer node.Close()

	assert.Equal(t, "node-1", node.ID())
}

// TestRaftV2Smoke_DefaultsAreV2 documents the M5 PR 28b completion of the
// phased flip: both serveruntime and cluster default to v2.
func TestRaftV2Smoke_DefaultsAreV2(t *testing.T) {
	t.Setenv("GRAINFS_RAFT_V2", "")
	resetRaftV2FlagForTest()
	t.Cleanup(resetRaftV2FlagForTest)

	assert.True(t, IsV2Enabled("serveruntime"),
		"expected serveruntime to default to v2 when GRAINFS_RAFT_V2 is unset")
	assert.True(t, IsV2Enabled("cluster"),
		"expected cluster to default to v2 when GRAINFS_RAFT_V2 is unset (PR 28b)")
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
// GRAINFS_RAFT_V2=serveruntime overrides the empty-env default (which would
// otherwise enable v2 for both pkgs after PR 28b) and pins cluster back to v1
// — a named-package list is exhaustive, not additive.
func TestRaftV2Smoke_OtherPkgFlagDoesNotAffectCluster(t *testing.T) {
	t.Setenv("GRAINFS_RAFT_V2", "serveruntime")
	resetRaftV2FlagForTest()
	t.Cleanup(resetRaftV2FlagForTest)

	rcfg := raft.DefaultConfig("node-1", nil)
	node, _, err := newRaftNode(rcfg, nil, "")
	require.NoError(t, err)

	// Must still be v1 — explicit serveruntime-only list excludes cluster.
	_, isV1 := node.(*raft.Node)
	assert.True(t, isV1, "GRAINFS_RAFT_V2=serveruntime must not enable v2 for cluster")

	node.SetTransport(noopRV, noopAE)
	node.Start()
	defer node.Close()

	os.Remove(node.ID()) // cleanup any state files
}

// TestRaftV2Smoke_ParseFlag verifies the flag parser handles all documented
// cases. As of M5 PR 28b an empty env defaults to both serveruntime=v2 and
// cluster=v2; "off" is the escape hatch that disables v2.
func TestRaftV2Smoke_ParseFlag(t *testing.T) {
	cases := []struct {
		name    string
		env     string
		cluster bool
		srv     bool
	}{
		{"empty_defaults_to_both_v2", "", true, true},
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
