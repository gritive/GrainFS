package scenarios

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/raft/chaos"
)

// TestChaos_LearnerFirst_LeaderChange_NewLeaderPromotes verifies that when a
// learner exists at the time of a leader change, the new leader's heartbeat
// watcher proposes PromoteToVoter for the learner. This is the chaos-harness
// version of TestAddVoter_E2E_LeaderChange_StillPromotes (skipped in
// integration_test.go due to QUIC transport timing fragility).
//
// Sequence:
//  1. 3-node cluster, AddLearner committed.
//  2. Old leader stops.
//  3. New leader is elected. Its watcher (heartbeat tick inline) sees the
//     learner caught up (low threshold) and proposes PromoteToVoter.
//  4. Cluster sees the learner promoted to voter.
func TestChaos_LearnerFirst_LeaderChange_NewLeaderPromotes(t *testing.T) {
	// 5-node so quorum (3) survives one node stop with margin — 3-node had
	// stickiness-window timing flakiness because the 2 surviving voters
	// occasionally took longer than 10s to elect a new leader.
	cluster := chaos.NewCluster(t, 5)
	cluster.StartAll()

	leader := cluster.WaitForLeader(5 * time.Second)
	require.NotNil(t, leader, "initial election")

	// Drive commitIndex up so AddLearner can commit.
	for i := 0; i < 5; i++ {
		require.NoError(t, leader.Propose([]byte("entry")))
	}

	// All nodes use a low threshold so a fresh learner with matchIndex=0 still
	// trivially passes (commitIndex < threshold).
	for _, n := range cluster.Nodes() {
		n.SetLearnerCatchupThreshold(1_000_000)
	}

	// AddLearner synchronously — commits before we proceed.
	require.NoError(t, leader.AddLearner("learner-x", "learner-x"))

	// Wait for AddLearner commit to propagate to all voter followers, otherwise
	// killing the leader strands the new leader at a stale commitIndex and the
	// AddLearner entry stays uncommitted (pendingConfChangeIndex never clears).
	leaderCommit := leader.CommittedIndex()
	require.Eventually(t, func() bool {
		for _, n := range cluster.Nodes() {
			if n.CommittedIndex() < leaderCommit {
				return false
			}
		}
		return true
	}, 5*time.Second, 50*time.Millisecond, "all nodes catch up to leader's commitIndex")

	// Stop the old leader. The chaos transport keeps it registered so RPC
	// targets resolve, but Close() makes it stop replying to AppendEntries.
	oldLeaderID := leader.ID()
	cluster.StopNode(oldLeaderID)

	// New leader must promote learner-x to voter.
	require.Eventually(t, func() bool {
		for _, n := range cluster.Nodes() {
			if n.ID() == oldLeaderID || !n.IsLeader() {
				continue
			}
			cfg := n.Configuration()
			for _, s := range cfg.Servers {
				if s.ID == "learner-x" && s.Suffrage == raft.Voter {
					return true
				}
			}
		}
		return false
	}, 10*time.Second, 50*time.Millisecond, "new leader's watcher must promote learner-x")
}

// TestChaos_LearnerFirst_SlowLearner_CallerCtxTimeout verifies that when a
// learner cannot catch up (network partitioned from the leader), AddVoterCtx
// fails with ctx.DeadlineExceeded and the learner remains in the cluster
// configuration (operator decides next step).
func TestChaos_LearnerFirst_SlowLearner_CallerCtxTimeout(t *testing.T) {
	cluster := chaos.NewCluster(t, 3)
	cluster.StartAll()

	leader := cluster.WaitForLeader(5 * time.Second)
	require.NotNil(t, leader)

	// Drive commitIndex high so the learner is far behind.
	for i := 0; i < 100; i++ {
		require.NoError(t, leader.Propose([]byte("entry")))
	}

	// Add a fresh learner-y, then partition it so it cannot catch up.
	newNode := cluster.AddNode("learner-y")
	require.NotNil(t, newNode)
	cluster.PartitionPeer("learner-y")

	// Strict threshold — far-behind learner cannot pass it.
	leader.SetLearnerCatchupThreshold(10)

	// Caller bounded by short ctx.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err := leader.AddVoterCtx(ctx, "learner-y", "learner-y")
	require.ErrorIs(t, err, context.DeadlineExceeded, "ctx timeout expected")

	// Learner-y must remain in configuration as NonVoter (operator decides).
	cfg := leader.Configuration()
	stillLearner := false
	for _, s := range cfg.Servers {
		if s.ID == "learner-y" && s.Suffrage == raft.NonVoter {
			stillLearner = true
		}
	}
	require.True(t, stillLearner, "slow learner must remain after ctx timeout")
}

// TestChaos_LearnerFirst_RepeatedLeaderChange_EventualPromote verifies that
// even with leader churn during catch-up, the watcher on the eventual stable
// leader will promote the learner.
func TestChaos_LearnerFirst_RepeatedLeaderChange_EventualPromote(t *testing.T) {
	cluster := chaos.NewCluster(t, 5)
	cluster.StartAll()

	leader := cluster.WaitForLeader(5 * time.Second)
	require.NotNil(t, leader)

	for i := 0; i < 10; i++ {
		require.NoError(t, leader.Propose([]byte("entry")))
	}

	for _, n := range cluster.Nodes() {
		n.SetLearnerCatchupThreshold(1_000_000)
	}

	// Add a learner-z synchronously.
	require.NoError(t, leader.AddLearner("learner-z", "learner-z"))

	// Wait for AddLearner commit propagation.
	leaderCommit := leader.CommittedIndex()
	require.Eventually(t, func() bool {
		for _, n := range cluster.Nodes() {
			if n.CommittedIndex() < leaderCommit {
				return false
			}
		}
		return true
	}, 5*time.Second, 50*time.Millisecond, "all nodes catch up")

	// Trigger 2 leader changes by isolating the current leader, healing, then
	// repeating with the next leader.
	for i := 0; i < 2; i++ {
		cur := cluster.WaitForLeader(2 * time.Second)
		if cur == nil {
			break
		}
		cluster.PartitionPeer(cur.ID())
		// Brief partition window — long enough for new election, short enough
		// to come back.
		time.Sleep(500 * time.Millisecond)
		cluster.HealPartition(cur.ID())
	}

	// Wait for the eventual stable leader's watcher to promote learner-z.
	require.Eventually(t, func() bool {
		stable := cluster.WaitForLeader(2 * time.Second)
		if stable == nil {
			return false
		}
		cfg := stable.Configuration()
		for _, s := range cfg.Servers {
			if s.ID == "learner-z" && s.Suffrage == raft.Voter {
				return true
			}
		}
		return false
	}, 15*time.Second, 100*time.Millisecond, "learner-z must eventually be promoted")
}
