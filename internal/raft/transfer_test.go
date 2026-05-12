package raft

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// timeoutNowRecorder wraps a Transport and records the peer that receives a
// SendTimeoutNow call. Used to verify target-selection logic.
type timeoutNowRecorder struct {
	inner  Transport
	target atomic.Pointer[string]
}

func (r *timeoutNowRecorder) SendRequestVote(peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	return r.inner.SendRequestVote(peer, args)
}
func (r *timeoutNowRecorder) SendAppendEntries(peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	return r.inner.SendAppendEntries(peer, args)
}
func (r *timeoutNowRecorder) SendInstallSnapshot(peer string, args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	return r.inner.SendInstallSnapshot(peer, args)
}
func (r *timeoutNowRecorder) SendTimeoutNow(peer string, args *TimeoutNowArgs) (*TimeoutNowReply, error) {
	r.target.Store(&peer)
	return r.inner.SendTimeoutNow(peer, args)
}

// TestTransferLeadership_ReturnsErrNoPeers verifies that TransferLeadership on a
// single-voter (solo) cluster returns ErrNoPeers immediately. A solo leader has
// no peers to transfer to so the call must be rejected without modifying state.
func TestTransferLeadership_ReturnsErrNoPeers(t *testing.T) {
	n, err := NewNode(Config{
		ID:              "solo",
		ElectionTimeout: fastElectionTimeout,
	})
	require.NoError(t, err)
	net := newMemNetwork()
	n.SetTransport(net.Register("solo", n))
	n.Start()
	t.Cleanup(n.Stop)
	go func() {
		for range n.ApplyCh() {
		}
	}()

	// Solo leader auto-bootstraps immediately.
	require.Eventually(t, n.IsLeader, 2*time.Second, 10*time.Millisecond, "solo node must be leader")

	got := n.TransferLeadership()
	require.Error(t, got)
	assert.True(t, errors.Is(got, ErrNoPeers), "expected ErrNoPeers, got: %v", got)
	// Must still be leader — the step-down should not have happened.
	assert.True(t, n.IsLeader(), "solo node must stay leader after ErrNoPeers")
}

// TestTransferLeadership_ReturnsErrNotLeaderOnFollower verifies that
// TransferLeadership returns ErrNotLeader when called on a node that is not the
// current leader.
func TestTransferLeadership_ReturnsErrNotLeaderOnFollower(t *testing.T) {
	nodes, _ := startCluster(t, "n1", "n2", "n3")
	n1 := nodes[0]

	require.Eventually(t, n1.IsLeader, 2*time.Second, 10*time.Millisecond, "n1 must be leader")

	// n2 is a follower.
	n2 := nodes[1]
	require.False(t, n2.IsLeader())

	got := n2.TransferLeadership()
	require.Error(t, got)
	assert.True(t, errors.Is(got, ErrNotLeader), "expected ErrNotLeader, got: %v", got)
}

// TestTransferLeadership_LeaderStepsDown verifies that the leader steps down to
// Follower after calling TransferLeadership. The leader must not be the leader
// after the call returns, and the cluster must elect a new leader.
func TestTransferLeadership_LeaderStepsDown(t *testing.T) {
	nodes, _ := startCluster(t, "n1", "n2", "n3")
	n1, n2, n3 := nodes[0], nodes[1], nodes[2]

	require.Eventually(t, n1.IsLeader, 2*time.Second, 10*time.Millisecond, "n1 must be leader")

	err := n1.TransferLeadership()
	require.NoError(t, err, "TransferLeadership must return nil on a multi-voter leader")

	// n1 must have stepped down.
	assert.False(t, n1.IsLeader(), "n1 must not be leader after TransferLeadership")

	// The cluster must elect a new leader within a reasonable timeout.
	require.Eventually(t, func() bool {
		return n2.IsLeader() || n3.IsLeader()
	}, 5*time.Second, 20*time.Millisecond, "a new leader must emerge after transfer")
}

// TestHandleTimeoutNow_FollowerBecomesCandidate verifies that a Follower that
// receives a TimeoutNow RPC transitions to Candidate (and eventually Leader in a
// single-voter scenario) immediately without waiting for its election timer.
func TestHandleTimeoutNow_FollowerBecomesCandidate(t *testing.T) {
	// Build a 3-voter cluster where n1 is the leader. We send TimeoutNow
	// directly to n2 (a follower) and verify it starts an election.
	nodes, _ := startCluster(t, "n1", "n2", "n3")
	n1 := nodes[0]
	n2 := nodes[1]

	require.Eventually(t, n1.IsLeader, 2*time.Second, 10*time.Millisecond, "n1 must be leader")

	term := n1.Term()

	// Send TimeoutNow to n2 directly (simulating what the leader would do).
	reply := n2.HandleTimeoutNow(&TimeoutNowArgs{
		Term:   term,
		Leader: "n1",
	})
	require.True(t, reply.Success, "n2 must accept TimeoutNow when it is a Follower")
}

// TestHandleTimeoutNow_StaleTermRejected verifies that a node rejects a
// TimeoutNow RPC whose term is lower than its own currentTerm. The node must
// not become a Candidate: Success=false and the node remains a Follower.
func TestHandleTimeoutNow_StaleTermRejected(t *testing.T) {
	nodes, _ := startCluster(t, "n1", "n2", "n3")
	n1 := nodes[0]
	n2 := nodes[1]

	require.Eventually(t, n1.IsLeader, 2*time.Second, 10*time.Millisecond, "n1 must be leader")

	// Advance n2's term via a higher-term RequestVote (so currentTerm > 1).
	currentTerm := n2.Term()
	require.Greater(t, currentTerm, uint64(0), "follower must have a term after election")

	// Send TimeoutNow with a term strictly less than n2's current term.
	staleTerm := currentTerm - 1
	if staleTerm == 0 {
		// If currentTerm is 1 we cannot go below; bump n2's term first.
		n2.HandleRequestVote(&RequestVoteArgs{
			Term:         currentTerm + 5,
			CandidateID:  "intruder",
			LastLogIndex: n2.CommittedIndex(),
			LastLogTerm:  currentTerm + 5,
		})
		currentTerm = n2.Term()
		staleTerm = currentTerm - 1
	}

	reply := n2.HandleTimeoutNow(&TimeoutNowArgs{
		Term:   staleTerm,
		Leader: "stale-leader",
	})
	assert.False(t, reply.Success, "stale-term TimeoutNow must be rejected")
	assert.GreaterOrEqual(t, reply.Term, currentTerm, "reply must carry n2's currentTerm")
	// n2 must not have started an election — it must still be a Follower.
	assert.False(t, n2.IsLeader(), "n2 must not become leader from a stale TimeoutNow")
}

// TestHandleTimeoutNow_LeaderIgnores verifies that a Leader that receives a
// TimeoutNow RPC ignores it (Success=false). A Leader is already leading and
// should not start a new election.
func TestHandleTimeoutNow_LeaderIgnores(t *testing.T) {
	nodes, _ := startCluster(t, "n1", "n2", "n3")
	n1 := nodes[0]

	require.Eventually(t, n1.IsLeader, 2*time.Second, 10*time.Millisecond, "n1 must be leader")

	// Send TimeoutNow to the leader itself — should be ignored.
	reply := n1.HandleTimeoutNow(&TimeoutNowArgs{
		Term:   n1.Term(),
		Leader: "n2", // simulated: some other node thinks it's leader
	})
	assert.False(t, reply.Success, "leader must ignore TimeoutNow (Success must be false)")
}

// TestTransferLeadership_TargetHasHigherMatchIndex verifies that TransferLeadership
// selects the peer with the highest matchIndex. In a 3-voter cluster where n2
// has replicated more entries than n3, TransferLeadership should send TimeoutNow
// to n2. The test selectively removes n3 from the memNetwork before proposing
// entries so that only n2 accumulates matchIndex, then restores n3 and wraps
// n1's transport with a recorder to observe which peer receives TimeoutNow.
func TestTransferLeadership_TargetHasHigherMatchIndex(t *testing.T) {
	// Build cluster: n1 (fast) leads, n2 and n3 are followers.
	nodes, net := startCluster(t, "n1", "n2", "n3")
	n1, n3 := nodes[0], nodes[2]

	require.Eventually(t, n1.IsLeader, 2*time.Second, 10*time.Millisecond, "n1 must be leader")

	// Remove n3 from the memNetwork so n1 cannot route RPCs to it. This causes
	// n1's AppendEntries to n3 to fail (ErrUnknownPeer), leaving n3's matchIndex
	// at 0 while n2 continues to receive entries. The cluster maintains quorum
	// with n1+n2 so entries commit.
	net.mu.Lock()
	delete(net.nodes, "n3")
	net.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for i := 0; i < 5; i++ {
		_, err := n1.ProposeWait(ctx, []byte("entry"))
		require.NoError(t, err, "proposal must succeed with n1+n2 quorum")
	}

	// Re-register n3 so it can receive TimeoutNow (though we expect n2 to be
	// selected, not n3).
	net.mu.Lock()
	net.nodes["n3"] = n3
	net.mu.Unlock()

	// Wrap n1's transport with the recorder. Re-registering n1 in the memNetwork
	// returns a fresh memTransport; the recorder delegates to it.
	innerTransport := net.Register("n1", n1)
	rec := &timeoutNowRecorder{inner: innerTransport}
	n1.SetTransport(rec)

	err := n1.TransferLeadership()
	require.NoError(t, err, "TransferLeadership must succeed")
	assert.False(t, n1.IsLeader(), "n1 must have stepped down")

	// The TimeoutNow is dispatched in a goroutine by the actor; wait for it.
	// n2 must be selected: matchIndex ≥ 5 while n3 was offline (matchIndex 0).
	require.Eventually(t, func() bool {
		return rec.target.Load() != nil
	}, 2*time.Second, 10*time.Millisecond, "TimeoutNow must have been sent to some peer")
	got := rec.target.Load()
	assert.Equal(t, "n2", *got, "n2 (highest matchIndex) must receive TimeoutNow")
}

// TestTransferLeadership_StoppedNodeReturnsErrNodeStopped verifies that calling
// TransferLeadership on a stopped node returns ErrNodeStopped without blocking.
func TestTransferLeadership_StoppedNodeReturnsErrNodeStopped(t *testing.T) {
	n, err := NewNode(Config{
		ID:              "solo",
		ElectionTimeout: fastElectionTimeout,
	})
	require.NoError(t, err)
	net := newMemNetwork()
	n.SetTransport(net.Register("solo", n))
	n.Start()
	go func() {
		for range n.ApplyCh() {
		}
	}()

	n.Stop()

	got := n.TransferLeadership()
	assert.True(t, errors.Is(got, ErrNodeStopped), "expected ErrNodeStopped, got: %v", got)
}
