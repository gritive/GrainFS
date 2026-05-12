package raft

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestLearnerPromote_LeaderStaysLeader_NoStepdown exercises the race the
// task report described: a solo-voter leader (n1) adds n2 as a learner,
// waits for it to catch up, then PromoteToVoter(n2). The hazard is that
// when n2 is promoted to voter (via the joint AddVoter phase 2 of v2's
// Path B), n2's election timer could fire before the leader's first
// heartbeat under the new voter epoch reaches it — causing n2 to campaign
// at a higher term and depose n1.
//
// Expected behaviour:
//   - PromoteToVoter returns nil
//   - n1 still IsLeader=true after promote completes
//   - n2 IsVoter in the resulting config
//
// Run with -count=100 to surface a statistical race.
func TestLearnerPromote_LeaderStaysLeader_NoStepdown(t *testing.T) {
	fix := startMembershipCluster(t, []string{"n1"})
	leader := fix.nodes[0]
	require.NoError(t, waitFor(2*time.Second, func() bool { return leader.IsLeader() }),
		"n1 must bootstrap as leader")
	leaderTermBefore := leader.Term()

	fix.addNode(t, "n2", []string{"n1"}, fastElectionTimeout)

	require.NoError(t, leader.AddLearner("n2", "n2-addr"))
	require.NoError(t, waitFor(3*time.Second, func() bool {
		return leader.peerMatchIndexForTest("n2") >= leader.CommittedIndex()
	}), "n2 must catch up to leader commit before promote")

	require.NoError(t, leader.PromoteToVoter("n2"), "PromoteToVoter must not fail (race regression)")

	require.True(t, leader.IsLeader(), "n1 must remain leader after promote (stepdown race)")
	require.Equal(t, leaderTermBefore, leader.Term(),
		"leader term must not advance — a stepdown+re-election would bump term")

	// Final config: 2 voters, no learners.
	cfg := leader.Configuration()
	require.Len(t, cfg.Servers, 2)
	for _, s := range cfg.Servers {
		require.Equal(t, Voter, s.Suffrage, "all servers must be Voter post-promote")
	}
}

// TestLearnerPromote_LeaderStaysLeader_3Voters runs the same scenario but
// starting from a 3-voter cluster + 1 learner → 4 voters. The race is the
// same — n4 might campaign right after its Cnew applies — but the leader
// has more peers, exercising the broadcastHeartbeat fan-out path.
func TestLearnerPromote_LeaderStaysLeader_3Voters(t *testing.T) {
	fix := startMembershipCluster(t, []string{"n1", "n2", "n3"})
	leader := fix.nodes[0]
	require.NoError(t, waitFor(2*time.Second, func() bool { return leader.IsLeader() }),
		"n1 must win election in 3-voter cluster")
	leaderTermBefore := leader.Term()

	fix.addNode(t, "n4", []string{"n1", "n2", "n3"}, fastElectionTimeout)

	require.NoError(t, leader.AddLearner("n4", "n4-addr"))
	require.NoError(t, waitFor(3*time.Second, func() bool {
		return leader.peerMatchIndexForTest("n4") >= leader.CommittedIndex()
	}), "n4 must catch up")

	require.NoError(t, leader.PromoteToVoter("n4"))
	require.True(t, leader.IsLeader(), "n1 must remain leader (3-voter promote race)")
	require.Equal(t, leaderTermBefore, leader.Term(),
		"leader term must not advance after 3-voter promote")

	cfg := leader.Configuration()
	require.Len(t, cfg.Servers, 4)
	for _, s := range cfg.Servers {
		require.Equal(t, Voter, s.Suffrage)
	}
}
