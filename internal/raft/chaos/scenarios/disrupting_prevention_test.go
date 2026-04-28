package scenarios

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/raft/chaos"
)

// TestDisruptingPrevention_HighTermVoteBlocked verifies that a follower
// which has recently received heartbeats from a live leader refuses a
// RequestVote with a higher term (leader stickiness / disrupting prevention).
//
// InjectRequestVote is used to bypass the transport layer and call
// HandleRequestVote directly, simulating a rogue candidate whose pre-vote
// succeeded in a different minority partition.
//
// Leader stickiness protects followers (not the leader itself): the leader
// has lastLeaderContact == zero because it never sends AppendEntries to
// itself. Followers, however, have a fresh lastLeaderContact from recent
// heartbeats and must reject real votes within ElectionTimeout.
func TestDisruptingPrevention_HighTermVoteBlocked(t *testing.T) {
	cluster := chaos.NewCluster(t, 3)
	cluster.StartAll()

	leader := cluster.WaitForLeader(5 * time.Second)
	require.NotNil(t, leader, "initial election must succeed")
	originalLeaderID := leader.ID()
	originalTerm := leader.Term()

	// Allow several heartbeats so followers have fresh lastLeaderContact.
	// NewCluster HeartbeatTimeout = 50ms; 150ms = 3 rounds.
	time.Sleep(150 * time.Millisecond)

	// Pick a follower as the injection target.
	var followerID string
	for _, id := range cluster.NodeIDs() {
		if id != originalLeaderID {
			followerID = id
			break
		}
	}
	require.NotEmpty(t, followerID)

	// Inject a real RequestVote (PreVote=false) with a much higher term directly
	// to the follower, bypassing all transport gating.
	// Stickiness must reject it: the follower heard from a leader within
	// ElectionTimeout (200ms in NewCluster).
	reply := cluster.InjectRequestVote(followerID, &raft.RequestVoteArgs{
		Term:        originalTerm + 10,
		CandidateID: "rogue",
		PreVote:     false,
	})
	require.NotNil(t, reply)
	assert.False(t, reply.VoteGranted, "stickiness must reject high-term vote from rogue candidate")

	// Give the cluster a moment to process any side-effects of the injection.
	time.Sleep(200 * time.Millisecond)

	current := cluster.CurrentLeader()
	require.NotNil(t, current, "cluster must still have a leader")
	assert.Equal(t, originalLeaderID, current.ID(),
		"original leader must remain after stickiness-rejected injection")
}
