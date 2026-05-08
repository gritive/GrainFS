package raftv2

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Election test timing knobs. Node 1 (the prospective leader) gets a much
// shorter election timeout than the others so the race is decisive: its
// randomized window [50,100)ms ends before the others' [300,600)ms even
// begins. Heartbeat interval is 30ms — well under any other node's election
// timeout, so followers do not time out while a leader is alive.
const (
	fastElectionTimeout = 50 * time.Millisecond
	slowElectionTimeout = 300 * time.Millisecond
	testHeartbeat       = 30 * time.Millisecond
)

// startCluster wires three Nodes through a memNetwork. n1 has a short election
// timeout; n2 and n3 have long ones. All three start as Follower; n1 wins the
// first election deterministically.
//
// The caller receives the three Nodes and a teardown closure registered via
// t.Cleanup. ApplyCh of each Node is drained in the background.
func startCluster(t *testing.T, ids ...string) (nodes []*Node, net *memNetwork) {
	t.Helper()
	require.Len(t, ids, 3, "startCluster expects exactly 3 ids")

	net = newMemNetwork()
	nodes = make([]*Node, 0, len(ids))

	for i, id := range ids {
		// Build peers list = ids except self.
		peers := make([]string, 0, len(ids)-1)
		for _, p := range ids {
			if p != id {
				peers = append(peers, p)
			}
		}

		electionTimeout := slowElectionTimeout
		if i == 0 {
			electionTimeout = fastElectionTimeout
		}
		n := NewNode(Config{
			ID:               id,
			Peers:            peers,
			ElectionTimeout:  electionTimeout,
			HeartbeatTimeout: testHeartbeat,
		})
		nodes = append(nodes, n)
	}

	// Register transports BEFORE starting actors so the first Candidate's
	// outbound RequestVote can route immediately.
	for _, n := range nodes {
		tr := net.Register(n.cfg.ID, n)
		n.SetTransport(tr)
	}
	for _, n := range nodes {
		n.Start()
		t.Cleanup(n.Stop)
		go func(n *Node) {
			for range n.ApplyCh() {
			}
		}(n)
	}
	return nodes, net
}

// TestElection_SingleCandidateWins: 3-node cluster, n1's short timeout makes
// it Candidate first. n2 and n3 grant their votes; n1 reaches majority (2/3)
// and becomes Leader.
func TestElection_SingleCandidateWins(t *testing.T) {
	nodes, _ := startCluster(t, "n1", "n2", "n3")
	n1, n2, n3 := nodes[0], nodes[1], nodes[2]

	require.NoError(t, waitFor(2*time.Second, func() bool {
		return n1.IsLeader()
	}), "n1 did not become leader")

	require.Equal(t, Leader, n1.State())
	require.Equal(t, "n1", n1.LeaderID())

	// Followers should converge on the new leader (via heartbeat).
	require.NoError(t, waitFor(2*time.Second, func() bool {
		return n2.LeaderID() == "n1" && n3.LeaderID() == "n1"
	}), "followers did not learn the leader")

	require.Equal(t, Follower, n2.State())
	require.Equal(t, Follower, n3.State())
}

// TestElection_HeartbeatPreventsReElection: once n1 is leader, periodic
// heartbeats keep n2/n3 election timers reset. The followers' term must not
// advance during a sustained leadership period.
func TestElection_HeartbeatPreventsReElection(t *testing.T) {
	nodes, _ := startCluster(t, "n1", "n2", "n3")
	n1, n2, n3 := nodes[0], nodes[1], nodes[2]

	require.NoError(t, waitFor(2*time.Second, func() bool {
		return n1.IsLeader() && n2.LeaderID() == "n1" && n3.LeaderID() == "n1"
	}), "cluster did not stabilise on n1")

	leaderTerm := n1.Term()

	// Watch for 500ms — well beyond a follower's election timeout window.
	// Heartbeats every 30ms must keep the followers parked.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		require.Equal(t, leaderTerm, n2.Term(), "n2 term must not advance under heartbeat")
		require.Equal(t, leaderTerm, n3.Term(), "n3 term must not advance under heartbeat")
		require.Equal(t, Follower, n2.State(), "n2 must remain Follower")
		require.Equal(t, Follower, n3.State(), "n3 must remain Follower")
		time.Sleep(20 * time.Millisecond)
	}
}

// TestElection_LeaderStepsDownOnHigherTerm: a Leader observing a higher-term
// RequestVote steps down to Follower at the new term. Inject the RequestVote
// directly via Handle so the test does not depend on a second cluster's
// election timing.
func TestElection_LeaderStepsDownOnHigherTerm(t *testing.T) {
	nodes, _ := startCluster(t, "n1", "n2", "n3")
	n1 := nodes[0]

	require.NoError(t, waitFor(2*time.Second, func() bool { return n1.IsLeader() }))
	leaderTerm := n1.Term()

	reply := n1.HandleRequestVote(&RequestVoteArgs{
		Term:         leaderTerm + 5,
		CandidateID:  "intruder",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	require.True(t, reply.VoteGranted, "leader with empty log must grant a higher-term vote")
	require.Equal(t, leaderTerm+5, reply.Term)
	require.Equal(t, Follower, n1.State(), "leader must step down")
	require.False(t, n1.IsLeader())
	require.Equal(t, leaderTerm+5, n1.Term())
	require.Equal(t, "intruder", n1.rs.Load().votedFor)
}

// TestElection_TermAgreementAcrossCluster: after election stabilises, all
// three nodes report the same term. Sanity check that the heartbeat path
// propagates the leader's term to followers.
func TestElection_TermAgreementAcrossCluster(t *testing.T) {
	nodes, _ := startCluster(t, "n1", "n2", "n3")
	n1, n2, n3 := nodes[0], nodes[1], nodes[2]

	require.NoError(t, waitFor(2*time.Second, func() bool {
		return n1.IsLeader() && n2.LeaderID() == "n1" && n3.LeaderID() == "n1"
	}))

	term := n1.Term()
	require.Equal(t, term, n2.Term(), "n2 term must match leader")
	require.Equal(t, term, n3.Term(), "n3 term must match leader")
	require.GreaterOrEqual(t, term, uint64(1), "election advances term at least once")
}
