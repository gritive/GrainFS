package raft

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestReadIndex_NotLeader: a Follower must reject ReadIndex with ErrNotLeader
// without queueing or any actor interaction.
func TestReadIndex_NotLeader(t *testing.T) {
	nodes, _ := startCluster(t, "n1", "n2", "n3")
	n1, n2 := nodes[0], nodes[1]

	require.NoError(t, waitFor(2*time.Second, func() bool { return n1.IsLeader() }))
	require.False(t, n2.IsLeader())

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	idx, err := n2.ReadIndex(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNotLeader)
	require.Equal(t, uint64(0), idx)
}

// TestReadIndex_SingleVoter: single-voter cluster has self-quorum at every
// commit; ReadIndex must return commitIndex inline without waiting on
// heartbeats (there are no peers).
func TestReadIndex_SingleVoter(t *testing.T) {
	n := startSingleVoter(t, "solo")

	pctx, pcancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer pcancel()
	idx, err := n.ProposeWait(pctx, []byte("v1"))
	require.NoError(t, err)
	require.Equal(t, uint64(1), idx, "single-voter skips becomeLeader no-op; first propose is index 1")

	rctx, rcancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer rcancel()
	got, err := n.ReadIndex(rctx)
	require.NoError(t, err)
	require.GreaterOrEqual(t, got, uint64(1), "barrier must reflect at least the proposed entry")
}

// TestReadIndex_MultiVoter_LeaderConfirmation: 3-node cluster, leader's
// ReadIndex blocks until a heartbeat round confirms majority, then returns
// barrier = commitIndex captured at request time.
func TestReadIndex_MultiVoter_LeaderConfirmation(t *testing.T) {
	nodes, _ := startCluster(t, "n1", "n2", "n3")
	n1 := nodes[0]
	require.NoError(t, waitFor(2*time.Second, func() bool { return n1.IsLeader() }))

	pctx, pcancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer pcancel()
	_, err := n1.ProposeWait(pctx, []byte("commit-1"))
	require.NoError(t, err)
	commitBefore := n1.CommittedIndex()
	require.GreaterOrEqual(t, commitBefore, uint64(1))

	rctx, rcancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer rcancel()
	idx, err := n1.ReadIndex(rctx)
	require.NoError(t, err)
	require.GreaterOrEqual(t, idx, commitBefore,
		"ReadIndex must return at least the commit index that existed at request time")
}

// TestReadIndex_MultiVoter_StepDownDrains: a queued ReadIndex on a partitioned
// leader must return ErrProposalFailed once the leader is forced to step down.
func TestReadIndex_MultiVoter_StepDownDrains(t *testing.T) {
	// Custom 3-voter setup: n1 leads but its outbound AE drops, so a queued
	// ReadIndex never confirms via heartbeat. Then we inject a higher-term AE
	// so n1 steps down — the queue must drain.
	net := newMemNetwork()
	ids := []string{"n1", "n2", "n3"}
	nodes := make([]*Node, 3)
	for i, id := range ids {
		peers := otherIDsLocal(ids, id)
		electionTimeout := slowElectionTimeout
		if id == "n1" {
			electionTimeout = fastElectionTimeout
		}
		n, nerr := NewNode(Config{
			ID:               id,
			Peers:            peers,
			ElectionTimeout:  electionTimeout,
			HeartbeatTimeout: testHeartbeat,
		})
		require.NoError(t, nerr)
		nodes[i] = n
	}
	for _, n := range nodes {
		tr := net.Register(n.cfg.ID, n)
		if n.cfg.ID == "n1" {
			n.SetTransport(&dropAETransport{inner: tr})
		} else {
			n.SetTransport(tr)
		}
	}
	for _, n := range nodes {
		n.Start()
		t.Cleanup(n.Stop)
		go func(n *Node) {
			for range n.ApplyCh() {
			}
		}(n)
	}

	n1 := nodes[0]
	require.NoError(t, waitFor(2*time.Second, func() bool { return n1.IsLeader() }))
	leaderTerm := n1.Term()

	type result struct {
		idx uint64
		err error
	}
	resCh := make(chan result, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		i, e := n1.ReadIndex(ctx)
		resCh <- result{i, e}
	}()

	time.Sleep(50 * time.Millisecond)

	reply := n1.HandleAppendEntries(&AppendEntriesArgs{
		Term:     leaderTerm + 5,
		LeaderID: "intruder",
	})
	require.True(t, reply.Success)
	require.Equal(t, Follower, n1.State())

	select {
	case r := <-resCh:
		require.Error(t, r.err)
		require.True(t,
			errors.Is(r.err, ErrProposalFailed) || errors.Is(r.err, ErrNotLeader),
			"ReadIndex after step-down must surface a leader-loss error, got: %v", r.err)
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("ReadIndex hung after Leader step-down — queue not drained")
	}
}

// TestReadIndex_StaleRoundReplyIgnored: peerLastRound advance must be gated
// on (state==Leader && cmd.hbTerm == currentTerm). A stale-term reply with a
// huge hbRoundID must NOT advance peerLastRound, otherwise re-leadership at
// a higher term could pre-satisfy a fresh ReadIndex with stale evidence.
//
// Direct state-machine test (no network, no Start): we hand-construct Leader
// state at term T2 and call handleHeartbeatReply with a synthetic term-T1
// reply. The actor function is the unit under test; goroutine scheduling is
// deliberately out of scope.
func TestReadIndex_StaleRoundReplyIgnored(t *testing.T) {
	n, err := NewNode(Config{
		ID:               "n1",
		Peers:            []string{"p1", "p2"},
		ElectionTimeout:  time.Hour,
		HeartbeatTimeout: testHeartbeat,
	})
	require.NoError(t, err)
	// Hand-build Leader state at term 5. We do NOT call Start() — handle*
	// are called directly so there's no race with the actor goroutine.
	n.st.currentTerm = 5
	n.st.state = Leader
	n.st.leaderID = "n1"
	n.st.matchIndex = map[string]uint64{"p1": 0, "p2": 0}
	n.st.nextIndex = map[string]uint64{"p1": 1, "p2": 1}
	n.st.peerInFlight = map[string]bool{"p1": false, "p2": false}
	n.st.peerLastRound = map[string]uint64{"p1": 0, "p2": 0}
	n.st.leaderRound = 0
	n.rs.Store(n.st.snapshot())

	// Inject a stale-term reply (term 4 < currentTerm 5) with a huge hbRoundID.
	// The handleHeartbeatReply stale-term gate must reject before peerLastRound
	// advances. If the gate were broken, peerLastRound["p1"] would jump to 999.
	n.handleHeartbeatReply(command{
		kind:           cmdHeartbeatReply,
		hbPeer:         "p1",
		hbTerm:         4,
		hbDispatchTerm: 4,
		hbRoundID:      999,
		hbSuccess:      true,
		hbMatchAfter:   0,
	})
	require.Equal(t, uint64(0), n.st.peerLastRound["p1"],
		"stale-term reply must NOT advance peerLastRound")
	require.Equal(t, uint64(0), n.st.peerLastRound["p2"],
		"untouched peer's peerLastRound stays 0")

	// Sanity: a same-term reply DOES advance peerLastRound.
	n.handleHeartbeatReply(command{
		kind:           cmdHeartbeatReply,
		hbPeer:         "p1",
		hbTerm:         5,
		hbDispatchTerm: 5,
		hbRoundID:      7,
		hbSuccess:      true,
		hbMatchAfter:   0,
	})
	require.Equal(t, uint64(7), n.st.peerLastRound["p1"],
		"same-term reply must advance peerLastRound")
}

// TestReadIndex_PeerLastRoundResetOnLeaderRegain: a node leads at term T1,
// steps down, leads again at term T2. peerLastRound from T1 must NOT be
// retained across the leadership transition — leaderRound restarts at 0 in
// becomeLeader and the map is re-allocated, so a fresh ReadIndex's
// minPeerRound cannot match a stale T1 round.
func TestReadIndex_PeerLastRoundResetOnLeaderRegain(t *testing.T) {
	nodes, _ := startCluster(t, "x1", "x2", "x3")
	leader := nodes[0]
	require.NoError(t, waitFor(2*time.Second, func() bool { return leader.IsLeader() }))
	leaderTerm := leader.Term()

	// Issue a ReadIndex to advance leaderRound + peerLastRound on the original term.
	pctx, pcancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer pcancel()
	_, err := leader.ReadIndex(pctx)
	require.NoError(t, err)

	// Force step-down via higher-term AE.
	reply := leader.HandleAppendEntries(&AppendEntriesArgs{
		Term:     leaderTerm + 10,
		LeaderID: "intruder",
	})
	require.True(t, reply.Success)
	require.Equal(t, Follower, leader.State())

	// Wait for any node to win the next election at a higher term.
	require.NoError(t, waitFor(5*time.Second, func() bool {
		for _, n := range nodes {
			if n.IsLeader() && n.Term() > leaderTerm {
				return true
			}
		}
		return false
	}), "no new leader emerged at higher term")

	var newLeader *Node
	for _, n := range nodes {
		if n.IsLeader() {
			newLeader = n
			break
		}
	}
	require.NotNil(t, newLeader)

	// Fresh ReadIndex on the new leader must succeed — proves becomeLeader
	// reset peerLastRound + leaderRound; otherwise either pre-satisfaction
	// (linearizability bug) or perpetual block (deadlock) would fire.
	rctx, rcancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer rcancel()
	idx, err := newLeader.ReadIndex(rctx)
	require.NoError(t, err)
	require.GreaterOrEqual(t, idx, uint64(0))
}

// TestApplyPipeline_DecoupledFromActor: with a slow FSM consumer holding
// the apply pipeline back, the actor's cmdCh must remain live so concurrent
// ReadIndex / Propose calls succeed within heartbeat-round latency. This
// exercises the actor → applyInCh → applyLoop → applyCh decoupling end-to-end:
// in the pre-PR direct-applyCh path, applyCh would fill (cap 64), the actor
// would block on send, cmdCh would back up, and a new ReadIndex's enqueue
// would time out under ctx pressure.
func TestApplyPipeline_DecoupledFromActor(t *testing.T) {
	net := newMemNetwork()
	ids := []string{"a1", "a2", "a3"}
	nodes := make([]*Node, 3)
	for i, id := range ids {
		peers := otherIDsLocal(ids, id)
		electionTimeout := slowElectionTimeout
		if i == 0 {
			electionTimeout = fastElectionTimeout
		}
		n, nerr := NewNode(Config{
			ID:               id,
			Peers:            peers,
			ElectionTimeout:  electionTimeout,
			HeartbeatTimeout: testHeartbeat,
		})
		require.NoError(t, nerr)
		nodes[i] = n
	}
	for _, n := range nodes {
		tr := net.Register(n.cfg.ID, n)
		n.SetTransport(tr)
	}
	for _, n := range nodes {
		n.Start()
		t.Cleanup(n.Stop)
	}

	leader := nodes[0]
	// Slow consumer on leader: 5ms per entry. 200 entries × 5ms = 1s of FSM work.
	go func() {
		for entry := range leader.ApplyCh() {
			_ = entry
			time.Sleep(5 * time.Millisecond)
		}
	}()
	for _, n := range nodes[1:] {
		go func(n *Node) {
			for range n.ApplyCh() {
			}
		}(n)
	}

	require.NoError(t, waitFor(2*time.Second, func() bool { return leader.IsLeader() }))

	const N = 200
	pctx, pcancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pcancel()
	for i := 0; i < N; i++ {
		_, err := leader.ProposeWait(pctx, []byte{byte(i)})
		require.NoError(t, err, "propose %d failed; actor likely wedged on slow FSM", i)
	}

	// Proposes have all committed. applyLoop is now ~1s into a slow drain.
	// Actor should still service cmdCh: a fresh ReadIndex must complete
	// well within ctx (heartbeat round is testHeartbeat=30ms + RTT << 200ms).
	rctx, rcancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer rcancel()
	idx, err := leader.ReadIndex(rctx)
	require.NoError(t, err, "ReadIndex during slow apply drain — actor wedged?")
	require.GreaterOrEqual(t, idx, uint64(N), "barrier must reflect all committed entries")
}

// TestApplyPipeline_OrderingUnderSlowFSM: slow FSM consumer; verify FIFO
// order preserved end-to-end even when applyLoop's internal buffer grows.
func TestApplyPipeline_OrderingUnderSlowFSM(t *testing.T) {
	n, err := NewNode(Config{ID: "n1"})
	require.NoError(t, err)
	n.Start()
	t.Cleanup(n.Stop)

	require.NoError(t, waitFor(time.Second, func() bool { return n.IsLeader() }))

	const N = 200
	delivered := make([]uint64, 0, N)
	deliveredMu := sync.Mutex{}
	done := make(chan struct{})
	go func() {
		count := 0
		for entry := range n.ApplyCh() {
			if entry.Type == LogEntryNoOp {
				continue
			}
			time.Sleep(time.Millisecond) // slow consumer
			deliveredMu.Lock()
			delivered = append(delivered, entry.Index)
			count++
			c := count
			deliveredMu.Unlock()
			if c >= N {
				close(done)
				return
			}
		}
	}()

	pctx, pcancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer pcancel()
	for i := 0; i < N; i++ {
		_, err := n.ProposeWait(pctx, []byte{byte(i)})
		require.NoError(t, err)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("apply consumer didn't see all %d entries", N)
	}

	deliveredMu.Lock()
	defer deliveredMu.Unlock()
	require.Len(t, delivered, N)
	for i := 1; i < len(delivered); i++ {
		require.Greater(t, delivered[i], delivered[i-1],
			"applyCh delivery must be FIFO by Index; got delivered[%d]=%d, delivered[%d]=%d",
			i-1, delivered[i-1], i, delivered[i])
	}
}
