package raft

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestLearner_SoloLeaderAddLearnerCommitsInline registers a learner on a
// 1-voter cluster (the leader is the sole voter, so quorum = {self} and
// the AddLearner entry commits inline at append). Verifies the
// Configuration includes the learner with NonVoter suffrage.
func TestLearner_SoloLeaderAddLearnerCommitsInline(t *testing.T) {
	n, err := NewNode(Config{
		ID:               "leader",
		Peers:            nil,
		ElectionTimeout:  fastElectionTimeout,
		HeartbeatTimeout: testHeartbeat,
	})
	require.NoError(t, err)
	t.Cleanup(n.Stop)
	n.Start()
	require.NoError(t, waitFor(2*time.Second, func() bool { return n.IsLeader() }))
	// Drain applyCh.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range n.ApplyCh() {
		}
	}()

	require.NoError(t, n.AddLearner("learner-1", "addr-1"))

	cfg := n.Configuration()
	var voters, learners []Server
	for _, s := range cfg.Servers {
		if s.Suffrage == Voter {
			voters = append(voters, s)
		} else {
			learners = append(learners, s)
		}
	}
	require.Len(t, voters, 1)
	require.Equal(t, "leader", voters[0].ID)
	require.Len(t, learners, 1)
	require.Equal(t, "learner-1", learners[0].ID)

	// Proposing a normal command must still commit on the leader alone —
	// the learner does not contribute to quorum.
	_, err = n.ProposeWait(context.Background(), []byte("hello"))
	require.NoError(t, err)
}

// TestLearner_AddRejectsDuplicate verifies that AddLearner fails when id
// is already a voter OR already a learner.
func TestLearner_AddRejectsDuplicate(t *testing.T) {
	n, err := NewNode(Config{
		ID:               "leader",
		Peers:            nil,
		ElectionTimeout:  fastElectionTimeout,
		HeartbeatTimeout: testHeartbeat,
	})
	require.NoError(t, err)
	t.Cleanup(n.Stop)
	n.Start()
	require.NoError(t, waitFor(2*time.Second, func() bool { return n.IsLeader() }))
	go func() {
		for range n.ApplyCh() {
		}
	}()

	require.NoError(t, n.AddLearner("x", "addr"))
	require.ErrorIs(t, n.AddLearner("x", "addr"), ErrAlreadyLearner)
	// Voter self: cannot be added as a learner either.
	require.ErrorIs(t, n.AddLearner("leader", "addr"), ErrAlreadyLearner)
}

// TestLearner_ElectionGuardRejectsLearnerSelf seeds a node whose live
// configuration places its own ID in the learners map and verifies the
// election timer does NOT promote it to Candidate. Path B keeps
// containsAnyVoter naturally voter-only; this test pins that invariant.
func TestLearner_ElectionGuardRejectsLearnerSelf(t *testing.T) {
	// Construct a config where currentConfig.voters = {"other"} and
	// learners = {"self"}. "self" must never run for office.
	n, err := NewNode(Config{
		ID:               "self",
		Peers:            []string{"other"},
		ElectionTimeout:  fastElectionTimeout,
		HeartbeatTimeout: testHeartbeat,
	})
	require.NoError(t, err)
	t.Cleanup(n.Stop)

	// Patch the seed config so "self" is a learner of {"other"}.
	n.st.currentConfig = effectiveConfig{
		joint:    false,
		voters:   []string{"other"},
		learners: map[string]string{"self": "addr"},
	}
	n.st.invalidatePeerSet()
	n.publish()

	n.Start()
	go func() {
		for range n.ApplyCh() {
		}
	}()

	// Wait significantly longer than the election timeout; "self" must
	// stay Follower.
	time.Sleep(5 * fastElectionTimeout)
	require.False(t, n.IsLeader(), "learner-self must not become leader")
	st := n.State()
	require.Equal(t, Follower, st, "learner-self must stay Follower")
}

// TestLearner_PromoteToVoterFlipsSuffrage extends the solo-leader fixture
// with PromoteToVoter and verifies the target ends up as a Voter (and the
// learners map is empty).
func TestLearner_PromoteToVoterFlipsSuffrage(t *testing.T) {
	fix := startMembershipCluster(t, []string{"n1"})
	leader := fix.nodes[0]
	require.NoError(t, waitFor(2*time.Second, func() bool { return leader.IsLeader() }))

	// Bring n2 up on the same network so it can replicate the AddLearner
	// entry before promotion.
	fix.addNode(t, "n2", []string{"n1"}, slowElectionTimeout)

	require.NoError(t, leader.AddLearner("n2", "n2-addr"))

	// Wait for n2 to catch up — easy on a fresh log; matchIndex must
	// reach the AddLearner entry's index.
	require.NoError(t, waitFor(3*time.Second, func() bool {
		return leader.peerMatchIndexForTest("n2") >= leader.CommittedIndex()
	}))

	require.NoError(t, leader.PromoteToVoter("n2"))

	// Verify the LEADER's local view.
	cfg := leader.Configuration()
	voterCount := 0
	for _, s := range cfg.Servers {
		require.Equal(t, Voter, s.Suffrage, "leader: all servers must be voters after promote")
		voterCount++
	}
	require.Equal(t, 2, voterCount)

	// Path B critical check: the FOLLOWER replays both entries
	// (PromoteStage1 + joint Cnew + joint exit) and observes itself as
	// a Voter. A bug in applyConfigEntry on the follower side would
	// silently pass the leader-only check above.
	var n2 *Node
	for _, n := range fix.nodes {
		if n.cfg.ID == "n2" {
			n2 = n
			break
		}
	}
	require.NotNil(t, n2)
	require.NoError(t, waitFor(3*time.Second, func() bool {
		got := n2.Configuration()
		if len(got.Servers) != 2 {
			return false
		}
		for _, s := range got.Servers {
			if s.Suffrage != Voter {
				return false
			}
		}
		return true
	}), "n2 must replay PromoteStage1 + joint AddVoter and see itself as Voter")
}

// TestLearner_PromoteNotALearner asserts that PromoteToVoter rejects an
// id that is not a learner.
func TestLearner_PromoteNotALearner(t *testing.T) {
	n, err := NewNode(Config{
		ID:               "leader",
		Peers:            nil,
		ElectionTimeout:  fastElectionTimeout,
		HeartbeatTimeout: testHeartbeat,
	})
	require.NoError(t, err)
	t.Cleanup(n.Stop)
	n.Start()
	require.NoError(t, waitFor(2*time.Second, func() bool { return n.IsLeader() }))
	go func() {
		for range n.ApplyCh() {
		}
	}()

	require.ErrorIs(t, n.PromoteToVoter("never-added"), ErrNotALearner)
}

// TestLearner_PromoteNotCaughtUp pins the catchup gate. The leader's
// configured threshold is small; we manually rewind the learner's
// matchIndex so the gate fires.
func TestLearner_PromoteNotCaughtUp(t *testing.T) {
	n, err := NewNode(Config{
		ID:                      "leader",
		Peers:                   nil,
		ElectionTimeout:         fastElectionTimeout,
		HeartbeatTimeout:        testHeartbeat,
		LearnerCatchupThreshold: 1,
	})
	require.NoError(t, err)
	t.Cleanup(n.Stop)
	n.Start()
	require.NoError(t, waitFor(2*time.Second, func() bool { return n.IsLeader() }))
	go func() {
		for range n.ApplyCh() {
		}
	}()
	require.NoError(t, n.AddLearner("L", "addr"))
	// Drive commit forward so commitIndex >> matchIndex[L]=AddLearner+1.
	for i := 0; i < 10; i++ {
		_, err := n.ProposeWait(context.Background(), []byte("x"))
		require.NoError(t, err)
	}
	// Force matchIndex[L] back to 0 via the test hook so the gate trips.
	n.setPeerMatchIndexForTest("L", 0)
	require.ErrorIs(t, n.PromoteToVoter("L"), ErrLearnerNotCaughtUp)
}
