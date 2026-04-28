package scenarios

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/raft/chaos"
)

// TestMixedVersionRollingUpgrade_PreVoteGracefulFallback simulates a rolling
// upgrade where node-2 is "old" software that silently drops all incoming
// PreRequestVote RPCs. The remaining two nodes (node-0 and node-1) form a
// pre-vote majority (2/3) among themselves, so the cluster still elects a
// leader without disruption.
//
// This closes the gap noted in TestMixedVersionRollingUpgrade_SameVersionBaseline:
// "PR 1a will extend this test to assert Pre-vote falls back gracefully when
// one of the three nodes responds 'unknown RPC' to PreRequestVote."
func TestMixedVersionRollingUpgrade_PreVoteGracefulFallback(t *testing.T) {
	cluster := chaos.NewCluster(t, 3)

	// Simulate the *receive* side of an old node: drop all incoming PreVote RPCs
	// delivered to node-2. This models the scenario where the old software simply
	// does not handle PreRequestVote and the RPC is silently discarded.
	// Note: node-2 still emits PreVote=true (it is the same binary), so this
	// is a partial simulation — it captures the "old node cannot receive PreVote"
	// case that matters for cluster availability. node-0 and node-1 form a 2/3
	// pre-vote majority among themselves and can proceed to election.
	cluster.SetRequestVoteHook("node-2", func(from, to string, args *raft.RequestVoteArgs) (*raft.RequestVoteArgs, bool) {
		if args.PreVote {
			return nil, true // drop incoming PreVote to node-2
		}
		return args, false
	})

	cluster.StartAll()

	leader := cluster.WaitForLeader(5 * time.Second)
	require.NotNil(t, leader, "cluster must elect a leader with one pre-vote-dropping node")

	// Allow the cluster to stabilize.
	time.Sleep(300 * time.Millisecond)

	current := cluster.CurrentLeader()
	require.NotNil(t, current, "cluster must still have a leader after stabilization")
	assert.Equal(t, leader.ID(), current.ID(), "no leader churn expected in a stable mixed-version cluster")
}

// TestMixedVersionRollingUpgrade_SameVersionBaseline establishes the rolling
// upgrade test scaffolding. Three nodes, all same version, restarted in
// sequence — cluster must re-elect a leader after each restart.
//
// PR 1a will extend this test (or add a sibling) to assert Pre-vote falls
// back gracefully when one of the three nodes responds "unknown RPC" to
// PreRequestVote, simulating a mixed old/new cluster during rolling upgrade.
//
// This test must remain PASSING across all subsequent raft PRs.
func TestMixedVersionRollingUpgrade_SameVersionBaseline(t *testing.T) {
	cluster := chaos.NewCluster(t, 3)
	cluster.StartAll()

	leader := cluster.WaitForLeader(5 * time.Second)
	require.NotNil(t, leader, "initial election must succeed")

	for _, nodeID := range cluster.NodeIDs() {
		cluster.RestartNode(nodeID)

		require.Eventually(t, func() bool {
			return cluster.CurrentLeader() != nil
		}, 5*time.Second, 50*time.Millisecond,
			"cluster must re-elect leader after restart of %s", nodeID)
	}
}
