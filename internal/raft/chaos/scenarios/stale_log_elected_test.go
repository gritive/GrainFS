package scenarios

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft/chaos"
)

// TestStaleLogElected_PreVotePreventsTermInflation verifies that a node
// partitioned for a long time does NOT inflate its term (thanks to Pre-vote)
// and therefore cannot disrupt the healthy leader after reconnection.
//
// With Pre-vote (PR 1a): the rogue's pre-vote round fails every cycle because
// it cannot reach quorum during partition — currentTerm stays put. After heal,
// it rejoins at the same term as the cluster and the original leader remains.
//
// Note: disrupting prevention (leader stickiness) is also active but never
// fires in this scenario because the rogue's term doesn't escalate. A separate
// chaos test for direct stickiness injection is planned as a follow-up.
func TestStaleLogElected_PreVotePreventsTermInflation(t *testing.T) {
	cluster := chaos.NewCluster(t, 3)
	cluster.StartAll()

	leader := cluster.WaitForLeader(5 * time.Second)
	require.NotNil(t, leader)
	originalLeaderID := leader.ID()

	// Pick a follower and partition it so its election timer fires repeatedly.
	var rogueID string
	for _, id := range cluster.NodeIDs() {
		if id != originalLeaderID {
			rogueID = id
			break
		}
	}
	require.NotEmpty(t, rogueID)

	cluster.PartitionPeer(rogueID)
	// Let the rogue sit in partition; pre-vote prevents term escalation.
	time.Sleep(2 * time.Second)

	// Heal. Pre-vote kept the rogue at its original term, so it rejoins cleanly.
	cluster.HealPartition(rogueID)
	time.Sleep(500 * time.Millisecond)

	current := cluster.CurrentLeader()
	require.NotNil(t, current)
	assert.Equal(t, originalLeaderID, current.ID(),
		"Pre-vote should keep the original leader after partition heal")
}
