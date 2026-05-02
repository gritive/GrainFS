package raft

import (
	"context"
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
	require.ElementsMatch(t, []string{"n1", "n2", "n3"}, view.currentVoters)
	require.Nil(t, view.oldVoters)
	require.Equal(t, map[string]string{"learner-1": "addr-l1"}, view.learnersByID)
	require.Equal(t, []string{"managed-1"}, view.managedLearnerIDs)
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
	require.Equal(t, []string{"n1", "n4", "n5"}, view.currentVoters)
	require.Equal(t, []string{"n1", "n2", "n3"}, view.oldVoters)
	require.Equal(t, uint64(42), view.enterIndex)
	require.Equal(t, []string{"n4"}, view.managedLearnerIDs)
}

func TestMembershipViewPublish_RemovedSelf(t *testing.T) {
	n := NewNode(Config{ID: "n1", Peers: []string{"n2", "n3"}})
	n.mu.Lock()
	n.removedFromCluster = true
	n.publishMembershipViewLocked()
	n.mu.Unlock()

	view := n.membershipViewSnapshot()
	require.Equal(t, []string{"n2", "n3"}, view.currentVoters)
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

	require.ElementsMatch(t, []string{"n1", "n2", "n3"}, view.currentVoters)
	require.Equal(t, map[string]string{"learner-1": "addr-l1"}, view.learnersByID)
	require.Empty(t, view.managedLearnerIDs)
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

func TestApplyConfigChange_NoOpDoesNotRepublishMembershipView(t *testing.T) {
	n := NewNode(Config{ID: "n1", Peers: []string{"n2", "n3"}})
	ch := make(chan error, 1)

	n.mu.Lock()
	view := n.membershipViewSnapshot()
	n.pendingReadIndex = []readIndexWaiter{{
		commitIndex: n.commitIndex,
		acked:       make(map[string]bool),
		ch:          ch,
		membership:  view,
	}}
	n.applyConfigChangeLocked(LogEntry{
		Index: 10,
		Term:  1,
		Type:  LogEntryConfChange,
		Command: encodeConfChange(ConfChangePayload{
			Op:      ConfChangeAddVoter,
			ID:      "n2",
			Address: "n2",
		}),
	})
	require.Same(t, view, n.membershipViewSnapshot())
	require.Len(t, n.pendingReadIndex, 1)
	n.mu.Unlock()

	select {
	case err := <-ch:
		t.Fatalf("no-op membership entry drained waiter with %v", err)
	default:
	}
}

func TestReadIndexWaiter_DrainedOnMembershipViewPublish(t *testing.T) {
	n := NewNode(Config{ID: "n1", Peers: []string{"n2", "n3"}})
	ch := make(chan error, 1)

	n.mu.Lock()
	n.pendingReadIndex = []readIndexWaiter{{
		commitIndex: n.commitIndex,
		acked:       make(map[string]bool),
		ch:          ch,
		membership:  n.membershipViewSnapshot(),
	}}
	n.applyConfigChangeLocked(LogEntry{
		Index: 11,
		Term:  1,
		Type:  LogEntryConfChange,
		Command: encodeConfChange(ConfChangePayload{
			Op:      ConfChangeAddVoter,
			ID:      "n4",
			Address: "n4",
		}),
	})
	require.Empty(t, n.pendingReadIndex)
	n.mu.Unlock()

	require.ErrorIs(t, <-ch, ErrMembershipChanged)
}

func TestReadIndexWaiter_NotDrainedOnNoOpMembershipEntry(t *testing.T) {
	n := NewNode(Config{ID: "n1", Peers: []string{"n2", "n3"}})
	ch := make(chan error, 1)

	n.mu.Lock()
	n.pendingReadIndex = []readIndexWaiter{{
		commitIndex: n.commitIndex,
		acked:       make(map[string]bool),
		ch:          ch,
		membership:  n.membershipViewSnapshot(),
	}}
	n.applyConfigChangeLocked(LogEntry{
		Index: 12,
		Term:  1,
		Type:  LogEntryConfChange,
		Command: encodeConfChange(ConfChangePayload{
			Op:      ConfChangeRemoveVoter,
			ID:      "missing",
			Address: "missing",
		}),
	})
	require.Len(t, n.pendingReadIndex, 1)
	n.mu.Unlock()

	select {
	case err := <-ch:
		t.Fatalf("no-op membership entry drained waiter with %v", err)
	default:
	}
}

func TestReadIndexWaiter_UsesCapturedMembershipView(t *testing.T) {
	n := NewNode(Config{ID: "n1", Peers: []string{"n2", "n3"}})
	ch := make(chan error, 1)

	n.mu.Lock()
	view := n.membershipViewSnapshot()
	n.pendingReadIndex = []readIndexWaiter{{
		commitIndex: n.commitIndex,
		acked:       make(map[string]bool),
		ch:          ch,
		membership:  view,
	}}
	n.config.Peers = []string{"n4", "n5"}
	n.tickReadIndexAcks("n2")
	require.Empty(t, n.pendingReadIndex)
	n.mu.Unlock()

	require.NoError(t, <-ch)
}

func TestQuorumMinMatchIndex_UsesSingleMembershipView(t *testing.T) {
	n := NewNode(Config{ID: "n1", Peers: []string{"n2", "n3"}})
	n.mu.Lock()
	n.matchIndex = map[string]uint64{
		"n1": 100,
		"n2": 80,
		"n3": 40,
		"n4": 100,
		"n5": 100,
	}
	n.publishMembershipViewLocked()

	n.config.Peers = []string{"n4", "n5"}
	require.Equal(t, uint64(80), n.quorumMinMatchIndexLocked())
	n.mu.Unlock()
}

func TestHandleInstallSnapshot_PublishesMembershipView(t *testing.T) {
	n := NewNode(Config{ID: "n1", Peers: []string{"n2"}})

	reply := n.HandleInstallSnapshot(&InstallSnapshotArgs{
		Term:              2,
		LeaderID:          "n3",
		LastIncludedIndex: 10,
		LastIncludedTerm:  2,
		Servers: []Server{
			{ID: "n2", Suffrage: Voter},
			{ID: "n3", Suffrage: Voter},
			{ID: "learner-1", Suffrage: NonVoter},
		},
	})
	require.Equal(t, uint64(2), reply.Term)

	view := n.membershipViewSnapshot()
	require.True(t, view.removedSelf)
	require.Equal(t, []string{"n2", "n3"}, view.peerVoters)
	require.Equal(t, []string{"n2", "n3"}, view.currentVoters)
	require.Equal(t, map[string]string{"learner-1": "learner-1"}, view.learnersByID)
}

func TestJointMembershipTransition_WithMembershipView(t *testing.T) {
	cluster := newTestCluster(t, 4)
	cluster.startAll()
	leader := cluster.waitForLeader(2 * time.Second)
	require.NotNil(t, leader)

	var removeID string
	leader.mu.Lock()
	for _, peer := range leader.config.Peers {
		removeID = peer
		break
	}
	leader.mu.Unlock()
	require.NotEmpty(t, removeID)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, leader.ChangeMembership(ctx, nil, []string{removeID}))

	require.Eventually(t, func() bool {
		view := leader.membershipViewSnapshot()
		return view.phase == JointNone && !containsPeer(view.currentVoters, removeID)
	}, 2*time.Second, 20*time.Millisecond)
}

func BenchmarkMembershipViewDualMajority_SingleMode(b *testing.B) {
	n := NewNode(Config{ID: "n1", Peers: []string{"n2", "n3", "n4", "n5"}})
	view := n.membershipViewSnapshot()
	acked := map[string]bool{"n2": true, "n3": true}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = view.dualMajority("n1", acked)
	}
}

func BenchmarkMembershipViewDualMajority_JointMode(b *testing.B) {
	n := NewNode(Config{ID: "n1", Peers: []string{"n2", "n3"}})
	n.mu.Lock()
	n.jointPhase = JointEntering
	n.jointOldVoters = []string{"n1", "n2", "n3"}
	n.jointNewVoters = []string{"n1", "n4", "n5"}
	n.publishMembershipViewLocked()
	n.mu.Unlock()
	view := n.membershipViewSnapshot()
	acked := map[string]bool{"n2": true, "n4": true}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = view.dualMajority("n1", acked)
	}
}

func BenchmarkHasQuorum_WithMembershipView(b *testing.B) {
	n := NewNode(Config{ID: "n1", Peers: []string{"n2", "n3", "n4", "n5"}})
	n.checkQuorumAcks = map[string]time.Time{
		"n2": time.Now(),
		"n3": time.Now(),
	}

	b.ReportAllocs()
	n.mu.Lock()
	defer n.mu.Unlock()
	for i := 0; i < b.N; i++ {
		_ = n.hasQuorum()
	}
}

func BenchmarkQuorumMinMatchIndex_WithMembershipView(b *testing.B) {
	n := NewNode(Config{ID: "n1", Peers: []string{"n2", "n3", "n4", "n5"}})
	n.matchIndex = map[string]uint64{
		"n1": 100,
		"n2": 90,
		"n3": 80,
		"n4": 70,
		"n5": 60,
	}

	b.ReportAllocs()
	n.mu.Lock()
	defer n.mu.Unlock()
	for i := 0; i < b.N; i++ {
		_ = n.quorumMinMatchIndexLocked()
	}
}

func serverIDs(servers []Server) []string {
	ids := make([]string, len(servers))
	for i, server := range servers {
		ids[i] = server.ID
	}
	return ids
}
