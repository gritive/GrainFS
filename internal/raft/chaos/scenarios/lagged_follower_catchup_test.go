package scenarios

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft/chaos"
)

// TestLaggedFollower_ConflictTermCatchup verifies that a follower with a
// large replication lag (12000 entries) can catch up quickly using
// ConflictTerm/ConflictIndex hints instead of one-by-one nextIndex decrement.
//
// Without fast backtracking: leader needs 12000 AppendEntries RPC round-trips
// to drive nextIndex from 12001 down to 1 — catastrophic in practice.
//
// With fast backtracking (PR 2): leader uses ConflictTerm hint to jump nextIndex
// to the correct position in O(1), then delivers all entries in a few batches.
//
// Acceptance for PR 2: this test must pass within 5s catch-up window.
func TestLaggedFollower_ConflictTermCatchup(t *testing.T) {
	cluster := chaos.NewCluster(t, 3)
	cluster.StartAll()

	leader := cluster.WaitForLeader(5 * time.Second)
	require.NotNil(t, leader, "cluster must elect a leader")

	// Find a follower to isolate.
	var laggedID string
	for _, id := range cluster.NodeIDs() {
		if id != leader.ID() {
			laggedID = id
			break
		}
	}

	// Partition the follower so it misses all new entries.
	cluster.PartitionPeer(laggedID)

	// Commit 12000 entries on the leader + remaining follower (quorum = 2/3).
	const entryCount = 12000
	for i := 0; i < entryCount; i++ {
		err := leader.Propose([]byte("x"))
		require.NoError(t, err, "propose must succeed with quorum of 2")
	}

	// Wait for the 12000 entries to be committed on the connected follower.
	require.Eventually(t, func() bool {
		for _, n := range cluster.Nodes() {
			if n.ID() != laggedID && n.CommittedIndex() >= uint64(entryCount) {
				return true
			}
		}
		return false
	}, 30*time.Second, 100*time.Millisecond,
		"connected nodes must commit all %d entries", entryCount)

	// Heal: lagged follower rejoins the network.
	cluster.HealPartition(laggedID)

	// The lagged follower must catch up within 5 seconds.
	// With O(N) backtracking this would take ~12000 heartbeat intervals (~600s).
	// With ConflictTerm O(1) jump + batched AE, it converges in a few RTTs.
	laggedNode := cluster.NodeByID(laggedID)
	require.NotNil(t, laggedNode)

	require.Eventually(t, func() bool {
		return laggedNode.CommittedIndex() >= uint64(entryCount)
	}, 5*time.Second, 50*time.Millisecond,
		"lagged follower must catch up to %d within 5s", entryCount)
}
