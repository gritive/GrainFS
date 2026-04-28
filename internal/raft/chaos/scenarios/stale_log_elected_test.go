package scenarios

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft/chaos"
)

// TestStaleLogElected_DisruptingPreventionRejects verifies that a node which
// has been partitioned for a long time (and thus has escalated its term
// repeatedly via failed elections) cannot disrupt a healthy leader after
// reconnection.
//
// Without Disrupting prevention (current state, PR 1a closes this): the
// rogue node's RequestVote with high term causes the leader to step down.
// Cluster churns through a re-election even though nothing was wrong.
//
// With Disrupting prevention (PR 1a): followers reject RequestVotes received
// within `electionTimeoutMin` of their last AppendEntries from the leader
// (leader stickiness). Combined with Pre-vote, this provides defense in
// depth against partitioned/removed nodes that won't shut up.
//
// Acceptance for PR 1a: remove the t.Skip line; this test must pass.
func TestStaleLogElected_DisruptingPreventionRejects(t *testing.T) {
	t.Skip("FAILING: closed by PR 1a Disrupting prevention — see docs/superpowers/plans/2026-04-29-raft-pr0-chaos-harness.md and design doc")

	cluster := chaos.NewCluster(t, 3)
	cluster.StartAll()

	leader := cluster.WaitForLeader(5 * time.Second)
	require.NotNil(t, leader)
	originalLeaderID := leader.ID()
	originalTerm := leader.Term()

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
	// Let the rogue go through many failed election cycles, escalating its term.
	time.Sleep(2 * time.Second)

	// Find rogue node.
	var rogueTerm uint64
	for _, n := range cluster.Nodes() {
		if n.ID() == rogueID {
			rogueTerm = n.Term()
			break
		}
	}
	require.Greater(t, rogueTerm, originalTerm+3,
		"rogue should have escalated term beyond leader's during partition")

	// Heal. Without Disrupting prevention, rogue's high-term RequestVote will
	// cause the leader to step down.
	cluster.HealPartition(rogueID)
	time.Sleep(500 * time.Millisecond)

	current := cluster.CurrentLeader()
	require.NotNil(t, current)
	assert.Equal(t, originalLeaderID, current.ID(),
		"Disrupting prevention should keep the original leader")
}
