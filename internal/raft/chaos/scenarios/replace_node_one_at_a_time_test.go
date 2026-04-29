package scenarios

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft/chaos"
)

// TestMembership_ReplaceNodeOneAtATime verifies single-server membership change:
// add one node, remove one node, cluster remains available throughout.
//
// This exercises the §4.4 append-path config apply, the pendingConfChangeIndex
// guard (no concurrent changes), and the quorum recalculation across the
// 3→4→3 node count transition.
func TestMembership_ReplaceNodeOneAtATime(t *testing.T) {
	cluster := chaos.NewCluster(t, 3)
	cluster.StartAll()

	leader := cluster.WaitForLeader(5 * time.Second)
	require.NotNil(t, leader, "initial election must succeed")

	// Baseline commit before membership changes.
	require.NoError(t, leader.Propose([]byte("baseline")))

	originalLeaderID := leader.ID()

	// Phase 1: Add a new node (node-3) to the cluster.
	newNode := cluster.AddNode("node-3")
	require.NotNil(t, newNode)

	require.NoError(t, leader.AddVoter("node-3", "node-3"),
		"AddVoter must be accepted by the leader")

	// Wait for the new node to receive and commit at least one entry via
	// AppendEntries. This confirms the leader is replicating to it.
	require.Eventually(t, func() bool {
		return newNode.CommittedIndex() > 0
	}, 5*time.Second, 50*time.Millisecond,
		"new node must catch up via AppendEntries")

	// Verify the 4-node cluster can still commit entries.
	require.NoError(t, leader.Propose([]byte("after-add")),
		"cluster must commit with 4 nodes")

	// Phase 2: Remove one of the original nodes (not the current leader).
	removeID := ""
	for _, id := range cluster.NodeIDs() {
		if id != originalLeaderID && id != "node-3" {
			removeID = id
			break
		}
	}
	require.NotEmpty(t, removeID)

	require.NoError(t, leader.RemoveVoter(removeID),
		"RemoveVoter must succeed")

	// Allow the removal entry to be committed and applied.
	time.Sleep(500 * time.Millisecond)

	// Partition and stop the removed node so it cannot interfere.
	cluster.PartitionPeer(removeID)
	cluster.StopNode(removeID)

	// The remaining 3-node cluster must still elect a leader and commit.
	remaining := cluster.WaitForLeader(5 * time.Second)
	require.NotNil(t, remaining, "3-node cluster after replacement must have a leader")
	require.NoError(t, remaining.Propose([]byte("after-remove")),
		"cluster must commit after node removal")
}
