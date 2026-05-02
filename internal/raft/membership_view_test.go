package raft

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMembershipViewPublish_SingleMode(t *testing.T) {
	n := NewNode(Config{ID: "n1", Peers: []string{"n2", "n3"}})
	n.mu.Lock()
	n.learnerIDs["learner-1"] = "addr-l1"
	n.jointManagedLearners["managed-1"] = struct{}{}
	n.publishMembershipViewLocked()
	n.mu.Unlock()

	view := n.membershipViewSnapshot()
	require.Equal(t, JointNone, view.phase)
	require.ElementsMatch(t, []string{"n1", "n2", "n3"}, view.current)
	require.Nil(t, view.old)
	require.Equal(t, map[string]string{"learner-1": "addr-l1"}, view.learners)
	require.Equal(t, []string{"managed-1"}, view.managedLearners)
	require.False(t, view.removedSelf)
}

func TestMembershipViewPublish_JointMode(t *testing.T) {
	n := NewNode(Config{ID: "n1", Peers: []string{"n2", "n3"}})
	n.mu.Lock()
	n.jointPhase = JointEntering
	n.jointOldVoters = []string{"n1", "n2", "n3"}
	n.jointNewVoters = []string{"n1", "n4", "n5"}
	n.jointEnterIndex = 42
	n.jointManagedLearners["n4"] = struct{}{}
	n.publishMembershipViewLocked()
	n.mu.Unlock()

	view := n.membershipViewSnapshot()
	require.Equal(t, JointEntering, view.phase)
	require.Equal(t, []string{"n1", "n4", "n5"}, view.current)
	require.Equal(t, []string{"n1", "n2", "n3"}, view.old)
	require.Equal(t, uint64(42), view.enterIndex)
	require.Equal(t, []string{"n4"}, view.managedLearners)
}

func TestMembershipViewPublish_RemovedSelf(t *testing.T) {
	n := NewNode(Config{ID: "n1", Peers: []string{"n2", "n3"}})
	n.mu.Lock()
	n.removedFromCluster = true
	n.publishMembershipViewLocked()
	n.mu.Unlock()

	view := n.membershipViewSnapshot()
	require.Equal(t, []string{"n2", "n3"}, view.current)
	require.True(t, view.removedSelf)
	require.NotContains(t, serverIDs(view.snapshotServers("n1")), "n1")
}

func TestMembershipViewImmutableAfterCanonicalMutation(t *testing.T) {
	n := NewNode(Config{ID: "n1", Peers: []string{"n2", "n3"}})
	n.mu.Lock()
	n.learnerIDs["learner-1"] = "addr-l1"
	n.publishMembershipViewLocked()
	view := n.membershipViewSnapshot()

	n.config.Peers[0] = "mutated"
	n.learnerIDs["learner-1"] = "mutated-l1"
	n.jointManagedLearners["managed-1"] = struct{}{}
	n.publishMembershipViewLocked()
	n.mu.Unlock()

	require.ElementsMatch(t, []string{"n1", "n2", "n3"}, view.current)
	require.Equal(t, map[string]string{"learner-1": "addr-l1"}, view.learners)
	require.Empty(t, view.managedLearners)
}

func TestMembershipViewDualMajority_NoMixedJointState(t *testing.T) {
	n := NewNode(Config{ID: "n1", Peers: []string{"n2", "n3"}})
	n.mu.Lock()
	n.jointPhase = JointEntering
	n.jointOldVoters = []string{"n1", "n2", "n3"}
	n.jointNewVoters = []string{"n1", "n4", "n5"}
	n.publishMembershipViewLocked()
	oldView := n.membershipViewSnapshot()

	n.jointOldVoters = []string{"n1", "old-new-a", "old-new-b"}
	n.jointNewVoters = []string{"n1", "n2", "n4"}
	n.publishMembershipViewLocked()
	n.mu.Unlock()

	require.False(t, oldView.dualMajority("n1", map[string]bool{"n2": true}),
		"old view must still require n4/n5 majority for C_new, not the later republished C_new")
	require.True(t, oldView.dualMajority("n1", map[string]bool{"n2": true, "n4": true}))
}

func TestJointSnapshotState_UsesSingleMembershipView(t *testing.T) {
	n := NewNode(Config{ID: "n1", Peers: []string{"n2", "n3"}})
	n.mu.Lock()
	n.jointPhase = JointEntering
	n.jointOldVoters = []string{"n1", "n2", "n3"}
	n.jointNewVoters = []string{"n1", "n4", "n5"}
	n.jointEnterIndex = 7
	n.jointManagedLearners["n4"] = struct{}{}
	n.publishMembershipViewLocked()

	n.jointOldVoters = []string{"n1", "x", "y"}
	n.jointNewVoters = []string{"n1", "a", "b"}
	n.jointEnterIndex = 9
	n.jointManagedLearners = map[string]struct{}{"later": {}}
	n.mu.Unlock()

	phase, oldVoters, newVoters, enterIndex, managedLearners := n.JointSnapshotState()
	require.Equal(t, int8(JointEntering), phase)
	require.Equal(t, []string{"n1", "n2", "n3"}, oldVoters)
	require.Equal(t, []string{"n1", "n4", "n5"}, newVoters)
	require.Equal(t, uint64(7), enterIndex)
	require.Equal(t, []string{"n4"}, managedLearners)
}

func TestApplyLoop_JointLeaveSelfRemoval_RepublishesMembershipView(t *testing.T) {
	n := NewNode(Config{ID: "n1", Peers: []string{"n2", "n3"}})
	n.mu.Lock()
	n.state = Leader
	n.jointPhase = JointEntering
	n.jointOldVoters = []string{"n1", "n2", "n3"}
	n.jointNewVoters = []string{"n2", "n3", "n4"}
	n.jointEnterIndex = 1
	n.commitIndex = 2
	n.log = []LogEntry{
		{Index: 1, Term: 1, Type: LogEntryJointConfChange, Command: encodeJointConfChange(JointConfChange{
			Op: JointOpEnter,
			OldServers: []ServerEntry{
				{ID: "n1", Suffrage: Voter}, {ID: "n2", Suffrage: Voter}, {ID: "n3", Suffrage: Voter},
			},
			NewServers: []ServerEntry{
				{ID: "n2", Suffrage: Voter}, {ID: "n3", Suffrage: Voter}, {ID: "n4", Suffrage: Voter},
			},
		})},
		{Index: 2, Term: 1, Type: LogEntryJointConfChange, Command: encodeJointConfChange(JointConfChange{
			Op: JointOpLeave,
			NewServers: []ServerEntry{
				{ID: "n2", Suffrage: Voter}, {ID: "n3", Suffrage: Voter}, {ID: "n4", Suffrage: Voter},
			},
		})},
	}
	n.publishMembershipViewLocked()
	n.mu.Unlock()

	n.wg.Add(1)
	go func() { defer n.wg.Done(); n.applyLoop() }()
	require.Eventually(t, func() bool {
		return n.membershipViewSnapshot().removedSelf
	}, time.Second, 10*time.Millisecond)
	n.Stop()
	n.wg.Wait()

	view := n.membershipViewSnapshot()
	require.True(t, view.removedSelf)
	require.NotContains(t, serverIDs(view.snapshotServers("n1")), "n1")
}

func TestRunPreVoteAndCandidate_CaptureMembershipWithTermLogView(t *testing.T) {
	for _, run := range []struct {
		name string
		fn   func(*Node)
	}{
		{name: "pre-vote", fn: func(n *Node) { n.runPreVote() }},
		{name: "candidate", fn: func(n *Node) { n.runCandidate() }},
	} {
		t.Run(run.name, func(t *testing.T) {
			n := NewNode(Config{
				ID:              "n1",
				Peers:           []string{"n2", "n3"},
				ElectionTimeout: 50 * time.Millisecond,
			})
			n.mu.Lock()
			n.state = Candidate
			n.currentTerm = 3
			n.jointPhase = JointEntering
			n.jointOldVoters = []string{"n1", "n2", "n3"}
			n.jointNewVoters = []string{"n1", "n4", "n5"}
			n.publishMembershipViewLocked()
			n.mu.Unlock()

			var mu sync.Mutex
			seen := map[string]bool{}
			mutated := false
			n.SetTransport(func(peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
				mu.Lock()
				seen[peer] = true
				if !mutated {
					mutated = true
					n.mu.Lock()
					n.jointOldVoters = []string{"n1", "later-old-a", "later-old-b"}
					n.jointNewVoters = []string{"n1", "later-new-a", "later-new-b"}
					n.publishMembershipViewLocked()
					n.mu.Unlock()
				}
				mu.Unlock()
				return &RequestVoteReply{Term: args.Term, VoteGranted: true}, nil
			}, nil)

			run.fn(n)

			mu.Lock()
			defer mu.Unlock()
			require.NotEmpty(t, seen)
			allowed := map[string]struct{}{"n2": {}, "n3": {}, "n4": {}, "n5": {}}
			for peer := range seen {
				require.Contains(t, allowed, peer)
			}
			require.NotContains(t, seen, "later-old-a")
			require.NotContains(t, seen, "later-new-a")
		})
	}
}

func TestHasQuorum_UsesPublishedMembershipView(t *testing.T) {
	n := NewNode(Config{
		ID:               "n1",
		Peers:            []string{"n2", "n3"},
		HeartbeatTimeout: 50 * time.Millisecond,
	})
	n.mu.Lock()
	n.jointPhase = JointEntering
	n.jointOldVoters = []string{"n1", "n2", "n3"}
	n.jointNewVoters = []string{"n1", "n4", "n5"}
	n.publishMembershipViewLocked()

	// Mutate canonical fields without publishing. hasQuorum must use the
	// previously published view, not rebuild from these later fields.
	n.jointOldVoters = []string{"n1", "later-old-a", "later-old-b"}
	n.jointNewVoters = []string{"n1", "later-new-a", "later-new-b"}
	n.checkQuorumAcks = map[string]time.Time{
		"n2": time.Now(),
		"n4": time.Now(),
	}
	require.True(t, n.hasQuorum())
	n.mu.Unlock()
}

func TestAdvanceCommitIndex_JointLeaveUsesPublishedMembershipView(t *testing.T) {
	n := jointTestNode("n1")
	n.config.Peers = []string{"n4", "n5"}
	n.jointPhase = JointNone
	n.currentTerm = 1
	n.commitIndex = 1
	n.firstIndex = 1
	n.log = []LogEntry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1, Type: LogEntryJointConfChange, Command: encodeJointConfChange(JointConfChange{
			Op:         JointOpLeave,
			OldServers: []ServerEntry{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}},
			NewServers: []ServerEntry{{ID: "n1"}, {ID: "n4"}, {ID: "n5"}},
		})},
	}
	n.matchIndex = map[string]uint64{"n4": 2}
	n.publishMembershipViewLocked()

	// Mutate canonical peers without publishing. The commit decision for this
	// entry must still use the captured C_new view (self+n4 over n1,n4,n5).
	n.config.Peers = []string{"n2", "n3"}
	n.advanceCommitIndex()
	require.Equal(t, uint64(2), n.commitIndex)
}

func TestMembershipViewHotReads_DoNotAllocate(t *testing.T) {
	n := NewNode(Config{ID: "n1", Peers: []string{"n2", "n3"}})
	n.mu.Lock()
	n.jointPhase = JointEntering
	n.jointOldVoters = []string{"n1", "n2", "n3"}
	n.jointNewVoters = []string{"n1", "n4", "n5"}
	n.publishMembershipViewLocked()
	n.mu.Unlock()

	view := n.membershipViewSnapshot()
	matched := map[string]bool{"n2": true, "n4": true}
	allocs := testing.AllocsPerRun(100, func() {
		_ = n.membershipViewSnapshot()
		_ = view.dualMajority("n1", matched)
	})
	require.Zero(t, allocs)
}

func serverIDs(servers []Server) []string {
	ids := make([]string, len(servers))
	for i, server := range servers {
		ids[i] = server.ID
	}
	return ids
}
