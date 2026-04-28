package scenarios

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft/chaos"
)

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
