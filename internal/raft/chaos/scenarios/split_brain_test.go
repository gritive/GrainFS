package scenarios

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft/chaos"
)

// TestSplitBrain_PreVotePreventsLeaderDisruption verifies that a partitioned
// follower returning to the cluster does NOT disrupt the existing leader.
//
// Without Pre-vote (current state, PR 1a closes this): the partitioned
// follower's election timer fires, it increments term, and on heal sends
// RequestVote with a higher term. The healthy leader sees the higher term
// and steps down — needless leader churn.
//
// With Pre-vote (PR 1a): the follower runs a pre-RequestVote round before
// incrementing term. Since the rest of the cluster is healthy, the pre-vote
// round fails to gather majority, and the follower stays Follower without
// disturbing the leader.
//
// Acceptance for PR 1a: remove the t.Skip line; this test must pass.
func TestSplitBrain_PreVotePreventsLeaderDisruption(t *testing.T) {

	cluster := chaos.NewCluster(t, 3)
	cluster.StartAll()

	leader := cluster.WaitForLeader(5 * time.Second)
	require.NotNil(t, leader, "initial election must succeed")
	originalLeaderID := leader.ID()

	// Pick a follower to partition.
	var followerID string
	for _, id := range cluster.NodeIDs() {
		if id != originalLeaderID {
			followerID = id
			break
		}
	}
	require.NotEmpty(t, followerID)

	// Partition and let the follower's election timer fire several times.
	cluster.PartitionPeer(followerID)
	time.Sleep(1 * time.Second) // ~5x ElectionTimeout

	// Heal — without Pre-vote, the follower's higher-term RequestVote
	// disrupts the leader.
	cluster.HealPartition(followerID)

	// Give the cluster a moment to re-stabilize.
	time.Sleep(500 * time.Millisecond)

	current := cluster.CurrentLeader()
	require.NotNil(t, current, "cluster must still have a leader")
	assert.Equal(t, originalLeaderID, current.ID(),
		"Pre-vote should prevent partitioned follower from disrupting the leader")
}
