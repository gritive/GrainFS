package raftv2

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// rpcTimeout bounds every reply wait so a misbehaving handler fails the test
// rather than hangs the test binary.
const rpcTimeout = 2 * time.Second

// awaitRequestVote synchronously dispatches a RequestVote and returns the
// reply, failing the test on timeout.
func awaitRequestVote(t *testing.T, n *Node, args *RequestVoteArgs) *RequestVoteReply {
	t.Helper()
	type result struct {
		reply *RequestVoteReply
	}
	ch := make(chan result, 1)
	go func() { ch <- result{n.HandleRequestVote(args)} }()
	select {
	case r := <-ch:
		require.NotNil(t, r.reply, "HandleRequestVote returned nil reply")
		return r.reply
	case <-time.After(rpcTimeout):
		t.Fatalf("HandleRequestVote timed out after %s", rpcTimeout)
		return nil
	}
}

// awaitAppendEntries is the AppendEntries counterpart to awaitRequestVote.
func awaitAppendEntries(t *testing.T, n *Node, args *AppendEntriesArgs) *AppendEntriesReply {
	t.Helper()
	type result struct {
		reply *AppendEntriesReply
	}
	ch := make(chan result, 1)
	go func() { ch <- result{n.HandleAppendEntries(args)} }()
	select {
	case r := <-ch:
		require.NotNil(t, r.reply, "HandleAppendEntries returned nil reply")
		return r.reply
	case <-time.After(rpcTimeout):
		t.Fatalf("HandleAppendEntries timed out after %s", rpcTimeout)
		return nil
	}
}

// startSingleVoter spins up a single-voter Node, waits for it to bootstrap
// to Leader at term 1, drains ApplyCh in the background, and registers
// teardown.
func startSingleVoter(t *testing.T, id string) *Node {
	t.Helper()
	n := NewNode(Config{ID: id})
	n.Start()
	t.Cleanup(n.Stop)
	t.Cleanup(func() {
		// Drain ApplyCh post-Stop in case any test left committed entries
		// undelivered. Stop closes the channel so this terminates.
	})
	go func() {
		for range n.ApplyCh() {
		}
	}()
	require.NoError(t, waitFor(time.Second, func() bool { return n.IsLeader() }))
	return n
}

// TestHandleRequestVote_GrantHappyPath: single-voter leader at term 1 receives
// a RequestVote at a higher term from an empty-log candidate. Per Raft §5.4
// the node must step down to Follower, advance to the higher term, and grant
// the vote (its own log is empty too, so the candidate is "as up-to-date").
func TestHandleRequestVote_GrantHappyPath(t *testing.T) {
	n := startSingleVoter(t, "n1")

	reply := awaitRequestVote(t, n, &RequestVoteArgs{
		Term:         2,
		CandidateID:  "other",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	require.True(t, reply.VoteGranted, "expected VoteGranted=true")
	require.Equal(t, uint64(2), reply.Term)

	require.Equal(t, Follower, n.State())
	require.Equal(t, uint64(2), n.Term())
	require.False(t, n.IsLeader())
	require.Equal(t, "other", n.rs.Load().votedFor)
}

// TestHandleRequestVote_DenyStaleTerm: a candidate with a lower term than
// ours must be denied without state change.
func TestHandleRequestVote_DenyStaleTerm(t *testing.T) {
	n := startSingleVoter(t, "n1")

	// Force the actor to term 5 by sending a higher-term step-down RPC,
	// then test rejection of a term-3 RequestVote.
	_ = awaitRequestVote(t, n, &RequestVoteArgs{
		Term:        5,
		CandidateID: "bumper",
	})
	require.Equal(t, uint64(5), n.Term())

	beforeVotedFor := n.rs.Load().votedFor
	beforeState := n.State()

	reply := awaitRequestVote(t, n, &RequestVoteArgs{
		Term:        3,
		CandidateID: "stale",
	})

	require.False(t, reply.VoteGranted)
	require.Equal(t, uint64(5), reply.Term)
	require.Equal(t, uint64(5), n.Term(), "term must not regress")
	require.Equal(t, beforeState, n.State())
	require.Equal(t, beforeVotedFor, n.rs.Load().votedFor, "votedFor must be unchanged on stale term")
}

// TestHandleRequestVote_DenyAlreadyVoted: at the same term, after voting for
// candidate A, a request from candidate B at the SAME term must be denied
// (split-vote prevention). We then verify that a request at a HIGHER term
// from B succeeds because the term advance clears votedFor.
func TestHandleRequestVote_DenyAlreadyVoted(t *testing.T) {
	n := startSingleVoter(t, "n1")

	// Step 1: term advances to 2, vote granted to "alice".
	r1 := awaitRequestVote(t, n, &RequestVoteArgs{
		Term:        2,
		CandidateID: "alice",
	})
	require.True(t, r1.VoteGranted)
	require.Equal(t, "alice", n.rs.Load().votedFor)

	// Step 2: same term, different candidate → deny.
	r2 := awaitRequestVote(t, n, &RequestVoteArgs{
		Term:        2,
		CandidateID: "bob",
	})
	require.False(t, r2.VoteGranted, "should deny: already voted for alice in term 2")
	require.Equal(t, uint64(2), r2.Term)
	require.Equal(t, "alice", n.rs.Load().votedFor, "votedFor must remain alice")

	// Step 3: higher term from bob → step down clears votedFor, grant.
	r3 := awaitRequestVote(t, n, &RequestVoteArgs{
		Term:        3,
		CandidateID: "bob",
	})
	require.True(t, r3.VoteGranted)
	require.Equal(t, uint64(3), r3.Term)
	require.Equal(t, "bob", n.rs.Load().votedFor)
}

// TestHandleRequestVote_DenyStaleLog: even with a clean votedFor slot, a
// candidate whose log is shorter / older than ours must be denied per §5.4.1.
//
// We seed the node's log via a controlled Propose at term 1 (single-voter
// auto-leader commits synchronously) so the node's lastLog{Index,Term} =
// {1, 1}. A candidate claiming lastLogIndex=0,lastLogTerm=0 at higher term
// must be denied.
func TestHandleRequestVote_DenyStaleLog(t *testing.T) {
	n := startSingleVoter(t, "n1")

	// Append one entry as the auto-leader so our log is non-empty.
	require.NoError(t, n.Propose([]byte("seed")))
	require.NoError(t, waitFor(time.Second, func() bool {
		return n.CommittedIndex() >= 1
	}))

	// Candidate has empty log → not as up-to-date → deny even at higher term.
	// Note: the term-step-down still occurs (Rule 2 runs before the log check),
	// so the node ends at term 3 / Follower / votedFor="".
	reply := awaitRequestVote(t, n, &RequestVoteArgs{
		Term:         3,
		CandidateID:  "shortlog",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	require.False(t, reply.VoteGranted, "candidate's log is shorter; vote must be denied")
	require.Equal(t, uint64(3), reply.Term)
	require.Equal(t, Follower, n.State())
	require.Equal(t, uint64(3), n.Term())
	require.Equal(t, "", n.rs.Load().votedFor, "votedFor cleared on step-down, not granted to stale-log candidate")
}

// TestHandleAppendEntries_HeartbeatStepDown: PR 5a accepts heartbeats. A
// higher-term AE forces step-down; a follow-up same-term heartbeat from the
// same leader is accepted and recorded as the current leader.
func TestHandleAppendEntries_HeartbeatStepDown(t *testing.T) {
	n := startSingleVoter(t, "n1")

	require.True(t, n.IsLeader())
	require.Equal(t, uint64(1), n.Term())

	reply := awaitAppendEntries(t, n, &AppendEntriesArgs{
		Term:     2,
		LeaderID: "newleader",
	})

	require.True(t, reply.Success, "PR 5a accepts heartbeats")
	require.Equal(t, uint64(2), reply.Term)
	require.Equal(t, Follower, n.State(), "must step down on higher-term AE")
	require.Equal(t, uint64(2), n.Term())
	require.Equal(t, "", n.rs.Load().votedFor, "votedFor cleared on step-down")
	require.Equal(t, "newleader", n.LeaderID(), "leader recognised on heartbeat")
	require.False(t, n.IsLeader())

	// Same-term heartbeat from the same leader: accepted, no state churn.
	reply2 := awaitAppendEntries(t, n, &AppendEntriesArgs{
		Term:     2,
		LeaderID: "newleader",
	})
	require.True(t, reply2.Success)
	require.Equal(t, uint64(2), reply2.Term)
	require.Equal(t, Follower, n.State())
	require.Equal(t, "newleader", n.LeaderID())

	// Stale-term AE: rejected, no state change.
	reply3 := awaitAppendEntries(t, n, &AppendEntriesArgs{
		Term:     1,
		LeaderID: "oldleader",
	})
	require.False(t, reply3.Success, "stale-term AE must be rejected")
	require.Equal(t, uint64(2), reply3.Term)
	require.Equal(t, "newleader", n.LeaderID(), "leaderID unchanged on stale AE")
}

// TestMemTransport_RoutesRequestVote: smoke test that memTransport actually
// dispatches into the destination Node's actor. Two single-voter nodes share
// a memNetwork; n1 sends RequestVote(term=2) to n2 via Transport.
func TestMemTransport_RoutesRequestVote(t *testing.T) {
	net := newMemNetwork()

	n1 := NewNode(Config{ID: "n1"})
	n2 := NewNode(Config{ID: "n2"})
	n1.Start()
	n2.Start()
	t.Cleanup(n1.Stop)
	t.Cleanup(n2.Stop)
	go func() {
		for range n1.ApplyCh() {
		}
	}()
	go func() {
		for range n2.ApplyCh() {
		}
	}()

	require.NoError(t, waitFor(time.Second, func() bool { return n2.IsLeader() }))

	tr := net.Register("n1", n1)
	net.Register("n2", n2)
	n1.SetTransport(tr)

	reply, err := tr.SendRequestVote("n2", &RequestVoteArgs{
		Term:        5,
		CandidateID: "n1",
	})
	require.NoError(t, err)
	require.True(t, reply.VoteGranted)
	require.Equal(t, uint64(5), reply.Term)
	require.Equal(t, uint64(5), n2.Term())
	require.Equal(t, Follower, n2.State())

	// Unknown peer returns ErrUnknownPeer.
	_, err = tr.SendRequestVote("ghost", &RequestVoteArgs{Term: 1})
	require.ErrorIs(t, err, ErrUnknownPeer)
}
