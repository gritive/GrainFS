package raft

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestInflightTracker_OutOfOrderAckDoesNotAdvancePastGap(t *testing.T) {
	tr := newInflightTracker(2, 0)

	require.True(t, tr.canSend(10))
	tr.sent(11, 20, 10)
	tr.sent(21, 30, 10)

	advanced, match := tr.ack(21, 30)
	require.False(t, advanced)
	require.Equal(t, uint64(0), match)

	advanced, match = tr.ack(11, 20)
	require.True(t, advanced)
	require.Equal(t, uint64(30), match)
}

func TestInflightTracker_StaleAckAfterResetIsIgnored(t *testing.T) {
	tr := newInflightTracker(2, 0)
	tr.sent(11, 20, 10)
	tr.reset(5)

	advanced, match := tr.ack(11, 20)
	require.False(t, advanced)
	require.Equal(t, uint64(0), match)
	require.True(t, tr.canSend(10))
}

func TestInflightTracker_ByteBudgetBlocksSend(t *testing.T) {
	tr := newInflightTracker(4, 10)

	require.True(t, tr.canSend(6))
	tr.sent(11, 12, 6)
	require.False(t, tr.canSend(5))
	require.True(t, tr.canSend(4))
}

func TestPeerReplicators_ReconcileAfterMembershipPublish(t *testing.T) {
	n := NewNode(Config{
		ID:                       "leader",
		Peers:                    []string{"a", "b"},
		MaxAppendEntriesInflight: 2,
	})
	n.mu.Lock()
	n.state = Leader
	n.currentTerm = 1
	n.peerReplicatorsActive = true
	n.publishMembershipViewLocked()
	require.Len(t, n.peerReplicators, 2)

	n.config.Peers = []string{"b", "c"}
	n.publishMembershipViewLocked()
	require.Len(t, n.peerReplicators, 2)
	_, hasA := n.peerReplicators["a"]
	_, hasB := n.peerReplicators["b"]
	_, hasC := n.peerReplicators["c"]
	require.False(t, hasA)
	require.True(t, hasB)
	require.True(t, hasC)

	n.peerReplicatorsActive = false
	n.stopPeerReplicatorsLocked()
	n.mu.Unlock()
	n.wg.Wait()
}

func TestPeerReplicator_AllowsTwoInflightAppendEntries(t *testing.T) {
	n := NewNode(Config{
		ID:                            "leader",
		Peers:                         []string{"follower"},
		MaxEntriesPerAE:               1,
		MaxAppendEntriesInflight:      2,
		MaxAppendEntriesInflightBytes: 0,
	})
	calls := make(chan *AppendEntriesArgs, 2)
	release := make(chan struct{})
	n.SetTransport(
		func(_ string, _ *RequestVoteArgs) (*RequestVoteReply, error) { return nil, nil },
		func(_ string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
			calls <- args
			<-release
			return &AppendEntriesReply{Term: args.Term, Success: true}, nil
		},
	)

	n.mu.Lock()
	n.state = Leader
	n.currentTerm = 1
	n.firstIndex = 1
	n.log = []LogEntry{
		{Index: 1, Term: 1, Command: []byte("a")},
		{Index: 2, Term: 1, Command: []byte("b")},
		{Index: 3, Term: 1, Command: []byte("c")},
	}
	n.nextIndex["follower"] = 1
	n.matchIndex["follower"] = 0
	r := newPeerReplicator(n, "follower")
	n.mu.Unlock()

	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		r.run()
	}()
	r.wake()

	first := requireReceiveAppend(t, calls)
	second := requireReceiveAppend(t, calls)
	require.Len(t, first.Entries, 1)
	require.Len(t, second.Entries, 1)
	got := map[uint64]bool{
		first.Entries[0].Index:  true,
		second.Entries[0].Index: true,
	}
	require.Equal(t, map[uint64]bool{1: true, 2: true}, got)

	close(release)
	r.stop()
	n.wg.Wait()
}

func TestPeerReplicator_HeartbeatConflictBacktracksNextIndex(t *testing.T) {
	n := NewNode(Config{ID: "leader", Peers: []string{"learner"}})
	n.mu.Lock()
	n.state = Leader
	n.currentTerm = 2
	n.firstIndex = 1
	n.log = []LogEntry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2},
	}
	n.nextIndex["learner"] = 4
	n.matchIndex["learner"] = 0
	r := newPeerReplicator(n, "learner")
	n.mu.Unlock()

	again := r.applyResult(replicationResult{
		term:          2,
		heartbeat:     true,
		success:       false,
		from:          4,
		conflictIndex: 1,
	})
	require.True(t, again)

	n.mu.Lock()
	defer n.mu.Unlock()
	require.Equal(t, uint64(1), n.nextIndex["learner"])
	require.Equal(t, uint64(1), r.sendNextIndex)
}

func TestPeerReplicator_ByteBudgetAllowsSingleOversizedEntry(t *testing.T) {
	n := NewNode(Config{
		ID:                            "leader",
		Peers:                         []string{"follower"},
		MaxAppendEntriesInflight:      2,
		MaxAppendEntriesInflightBytes: 1,
	})
	calls := make(chan *AppendEntriesArgs, 1)
	n.SetTransport(
		func(_ string, _ *RequestVoteArgs) (*RequestVoteReply, error) { return nil, nil },
		func(_ string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
			calls <- args
			return &AppendEntriesReply{Term: args.Term, Success: true}, nil
		},
	)

	n.mu.Lock()
	n.state = Leader
	n.currentTerm = 1
	n.firstIndex = 1
	n.log = []LogEntry{{Index: 1, Term: 1, Command: []byte("entry larger than the byte budget")}}
	n.nextIndex["follower"] = 1
	n.matchIndex["follower"] = 0
	r := newPeerReplicator(n, "follower")
	n.mu.Unlock()

	require.True(t, r.sendOne())
	args := requireReceiveAppend(t, calls)
	require.Len(t, args.Entries, 1)
	require.Equal(t, uint64(1), args.Entries[0].Index)
	n.wg.Wait()
}

func TestPeerReplicator_HigherTermReplyStepsDownLeader(t *testing.T) {
	n := NewNode(Config{ID: "leader", Peers: []string{"follower"}})
	n.mu.Lock()
	n.state = Leader
	n.currentTerm = 3
	n.votedFor = "leader"
	r := newPeerReplicator(n, "follower")
	n.mu.Unlock()

	again := r.applyResult(replicationResult{term: 4, from: 1, to: 1})
	require.False(t, again)

	n.mu.Lock()
	defer n.mu.Unlock()
	require.Equal(t, Follower, n.state)
	require.Equal(t, uint64(4), n.currentTerm)
	require.Empty(t, n.votedFor)
}

func TestPeerReplicator_SnapshotErrorClearsExclusiveState(t *testing.T) {
	n := NewNode(Config{ID: "leader", Peers: []string{"follower"}})
	r := newPeerReplicator(n, "follower")
	r.snapshotting = true

	again := r.applyResult(replicationResult{
		term:     1,
		snapshot: true,
		err:      errors.New("snapshot failed"),
	})
	require.False(t, again)
	require.False(t, r.snapshotting)
}

func requireReceiveAppend(t *testing.T, ch <-chan *AppendEntriesArgs) *AppendEntriesArgs {
	t.Helper()
	select {
	case args := <-ch:
		return args
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for AppendEntries")
		return nil
	}
}
