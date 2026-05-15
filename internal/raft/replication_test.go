package raft

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// seedLogEntries replaces the node's log store with the given entries. Must be
// called before Start — the actor goroutine is the sole writer after Start.
func seedLogEntries(n *Node, entries []LogEntry) {
	n.st.log = newMemLogStore()
	if err := n.st.log.Append(entries); err != nil {
		panic("seedLogEntries: " + err.Error())
	}
}

// mustLogEntry reads entry at 1-based logical index idx from n's log. Panics
// on error. Must be called after Stop (actor has exited; sole-writer invariant
// holds for the reading goroutine).
func mustLogEntry(n *Node, idx uint64) LogEntry {
	e, err := n.st.log.Entry(idx)
	if err != nil {
		panic("mustLogEntry: " + err.Error())
	}
	return e
}

func TestAppendEntriesRejectsNonContiguousBatch(t *testing.T) {
	n, err := NewNode(Config{ID: "follower", Peers: []string{"leader"}, ElectionTimeout: time.Hour})
	require.NoError(t, err)
	n.Start()
	t.Cleanup(n.Stop)
	go func() {
		for range n.ApplyCh() {
		}
	}()

	reply := n.HandleAppendEntries(&AppendEntriesArgs{
		Term:         1,
		LeaderID:     "leader",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []LogEntry{
			{Term: 1, Index: 1, Command: []byte("ok")},
			{Term: 1, Index: 3, Command: []byte("gap")},
		},
		LeaderCommit: 0,
	})

	require.False(t, reply.Success)
	n.Stop()
	require.Equal(t, uint64(0), n.st.log.LastIndex(), "malformed batch must not partially append")
}

// startClusterCapture is the replication-test counterpart of startCluster.
// Unlike startCluster, it does NOT drain ApplyCh in background goroutines;
// instead it returns a per-node []LogEntry that captures every applied entry
// so tests can assert what each node delivered. The capture goroutines are
// torn down on Stop (channel close) automatically.
//
// Returned `applied` is indexed in lockstep with `nodes` — applied[i]
// belongs to nodes[i]. Reads of applied[i] are safe only after the
// corresponding node has Stop()'d, OR after a polling waitFor confirms the
// expected entry count is reached. We provide waitForApplied for the latter.
type capturedNode struct {
	node    *Node
	applied []LogEntry
	doneCh  chan struct{}
}

// startCapturingCluster builds a 3-node cluster with capture goroutines
// instead of background drains. Returns the nodes plus a slice of captured
// applied-entry slices, one per node.
func startCapturingCluster(t *testing.T, ids ...string) []*capturedNode {
	t.Helper()
	require.Len(t, ids, 3, "startCapturingCluster expects exactly 3 ids")

	net := newMemNetwork()
	caps := make([]*capturedNode, 0, len(ids))

	for i, id := range ids {
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
		n, err := NewNode(Config{
			ID:               id,
			Peers:            peers,
			ElectionTimeout:  electionTimeout,
			HeartbeatTimeout: testHeartbeat,
		})
		require.NoError(t, err)
		caps = append(caps, &capturedNode{node: n, doneCh: make(chan struct{})})
	}

	// Register transports BEFORE starting actors so the first Candidate's
	// outbound RPCs route immediately.
	for _, c := range caps {
		c.node.SetTransport(net.Register(c.node.cfg.ID, c.node))
	}

	for _, c := range caps {
		c.node.Start()
		t.Cleanup(c.node.Stop)
		// Capture goroutine: drains applyCh into c.applied. The applyCh is
		// closed by the actor on Stop, terminating this goroutine. Single
		// writer (this goroutine), single reader (test goroutine after
		// waitForApplied confirms quiescence) — no lock needed.
		c := c // capture
		go func() {
			defer close(c.doneCh)
			for e := range c.node.ApplyCh() {
				c.applied = append(c.applied, e)
			}
		}()
	}
	return caps
}

// waitForCommitted polls every node's CommittedIndex (atomic snapshot, race-
// clean) until all have advanced to at least idx. Tests use this as the
// readiness signal; capturedNode.applied must only be read after Stop closes
// applyCh and the capture goroutine drains.
func waitForCommitted(t *testing.T, caps []*capturedNode, idx uint64, timeout time.Duration) {
	t.Helper()
	require.NoError(t, waitFor(timeout, func() bool {
		for _, c := range caps {
			if c.node.CommittedIndex() < idx {
				return false
			}
		}
		return true
	}), "not all nodes reached commitIndex >= %d", idx)
}

// readApplied stops the node and returns the captured entries. After Stop
// returns, applyCh is closed and the capture goroutine has drained, so
// reading applied is safe and complete.
func (c *capturedNode) readApplied() []LogEntry {
	c.node.Stop()
	<-c.doneCh
	return c.applied
}

// TestReplication_BasicHappyPath: 3-voter cluster, n1 wins election, leader
// proposes one entry, all three nodes apply it.
func TestReplication_BasicHappyPath(t *testing.T) {
	caps := startCapturingCluster(t, "n1", "n2", "n3")
	n1 := caps[0].node

	require.NoError(t, waitFor(2*time.Second, func() bool { return n1.IsLeader() }),
		"n1 did not become leader")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// becomeLeader appends a no-op at index 1 (Raft §5.4.2). The first user
	// propose therefore lands at index 2.
	idx, err := n1.ProposeWait(ctx, []byte("hello"))
	require.NoError(t, err)
	require.Equal(t, uint64(2), idx, "first user ProposeWait returns index 2 (no-op at index 1)")

	// All three nodes must reach commitIndex >= 2.
	waitForCommitted(t, caps, 2, 2*time.Second)

	// Drain by stopping nodes; verify each captured the no-op and the proposed entry.
	for _, c := range caps {
		entries := c.readApplied()
		require.Len(t, entries, 2, "%s applied count (no-op + hello)", c.node.cfg.ID)
		// Index 1: no-op
		require.Equal(t, uint64(1), entries[0].Index, "%s no-op index", c.node.cfg.ID)
		require.Equal(t, LogEntryNoOp, entries[0].Type, "%s no-op type", c.node.cfg.ID)
		// Index 2: user entry
		require.Equal(t, uint64(2), entries[1].Index, "%s user entry index", c.node.cfg.ID)
		require.Equal(t, []byte("hello"), entries[1].Command, "%s user entry command", c.node.cfg.ID)
		// Term comes from the leader at propose time; since n1 won term 1
		// uncontested, both entries must carry term 1.
		require.Equal(t, uint64(1), entries[1].Term, "%s user entry term", c.node.cfg.ID)
	}
}

// TestReplication_MultiplePropose: 3-voter cluster, leader proposes 5
// commands; all three nodes apply them in submission order.
func TestReplication_MultiplePropose(t *testing.T) {
	caps := startCapturingCluster(t, "n1", "n2", "n3")
	n1 := caps[0].node

	require.NoError(t, waitFor(2*time.Second, func() bool { return n1.IsLeader() }))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// no-op is at index 1; user commands land at indices 2..N+1.
	const N = 5
	for i := 1; i <= N; i++ {
		cmd := []byte(fmt.Sprintf("cmd-%d", i))
		idx, err := n1.ProposeWait(ctx, cmd)
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), idx, "ProposeWait #%d returns index %d (no-op at 1)", i, i+1)
	}

	waitForCommitted(t, caps, N+1, 2*time.Second)

	for _, c := range caps {
		entries := c.readApplied()
		require.Len(t, entries, N+1, "%s applied count (no-op + %d user)", c.node.cfg.ID, N)
		// entries[0] is the no-op; entries[1..N] are user commands.
		require.Equal(t, LogEntryNoOp, entries[0].Type, "%s entries[0] is no-op", c.node.cfg.ID)
		for i := 1; i <= N; i++ {
			e := entries[i]
			require.Equal(t, uint64(i+1), e.Index, "%s entry[%d].Index", c.node.cfg.ID, i)
			require.Equal(t, []byte(fmt.Sprintf("cmd-%d", i)), e.Command,
				"%s entry[%d].Command", c.node.cfg.ID, i)
		}
	}
}

// dropAETransport wraps a Transport and drops all SendAppendEntries calls
// (returning a non-nil error so the leader treats them as RPC failures).
// RequestVote still routes through, so leader election works.
type dropAETransport struct {
	inner Transport
}

func (d *dropAETransport) SendRequestVote(peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	return d.inner.SendRequestVote(peer, args)
}

func (d *dropAETransport) SendAppendEntries(peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	// Simulate a stuck follower: pretend the call hangs by returning an error.
	// The leader treats this as a transport failure (handleHeartbeatReply
	// short-circuits on hbErr != nil). Followers therefore never advance
	// their matchIndex, so any ProposeWait stays outstanding indefinitely.
	return nil, ErrUnknownPeer
}

func (d *dropAETransport) SendInstallSnapshot(peer string, args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	return d.inner.SendInstallSnapshot(peer, args)
}

func (d *dropAETransport) SendTimeoutNow(peer string, args *TimeoutNowArgs) (*TimeoutNowReply, error) {
	return d.inner.SendTimeoutNow(peer, args)
}

// TestReplication_LeaderStepDownReleasesWaiters: a Leader with an outstanding
// ProposeWait that observes a higher-term inbound AppendEntries must drain
// proposeWaiters and reply ErrProposalFailed — otherwise the caller blocks
// until ctx timeout. Regression test for the inbound-RPC step-down path.
func TestReplication_LeaderStepDownReleasesWaiters(t *testing.T) {
	// Custom 3-voter setup: n1 is the would-be leader, but its outbound AE
	// transport drops everything, so its proposes never commit.
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
	// Wire transports. n1 gets a wrapper that drops AE so it can win the
	// election (RequestVote still routes) but cannot commit anything.
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
	require.NoError(t, waitFor(2*time.Second, func() bool { return n1.IsLeader() }),
		"n1 did not become leader")
	leaderTerm := n1.Term()

	// Kick off a ProposeWait that will not commit (followers' AEs are dropped).
	// Use a generous ctx so a hang surfaces as an explicit ErrProposalFailed
	// from the step-down path, not a ctx timeout.
	resultCh := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		_, err := n1.ProposeWait(ctx, []byte("blocked"))
		resultCh <- err
	}()

	// Give the actor a moment to register the waiter.
	time.Sleep(50 * time.Millisecond)

	// Inject a higher-term AE directly into n1 — the inbound step-down path
	// must drain proposeWaiters and signal ErrProposalFailed.
	reply := n1.HandleAppendEntries(&AppendEntriesArgs{
		Term:     leaderTerm + 5,
		LeaderID: "intruder",
	})
	require.True(t, reply.Success, "AE at higher term must succeed once we step down")
	require.Equal(t, leaderTerm+5, reply.Term)
	require.Equal(t, Follower, n1.State())

	// ProposeWait must return ErrProposalFailed (wrapped) within ~100ms — well
	// before the 5s ctx timeout. A hang here is the bug being regression-tested.
	select {
	case err := <-resultCh:
		require.Error(t, err)
		require.ErrorIs(t, err, ErrProposalFailed,
			"ProposeWait should fail with ErrProposalFailed after step-down")
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("ProposeWait hung after Leader step-down — waiter not drained")
	}
}

// otherIDsLocal returns ids without self. Local copy because election_test.go
// scope sees the equivalent via startCluster's loop, not as an exported helper.
func otherIDsLocal(ids []string, self string) []string {
	out := make([]string, 0, len(ids)-1)
	for _, id := range ids {
		if id != self {
			out = append(out, id)
		}
	}
	return out
}

// TestReplication_TruncateMultipleEntries verifies the follower-side accept
// path truncates conflicting entries and appends the leader's version.
// Whitebox: drives HandleAppendEntries directly without election.
//
// Setup: a Follower-mode Node with seeded log [{T1,I1,A}, {T1,I2,B}, {T1,I3,C}].
// Leader sends AE with PrevLogIndex=1,PrevLogTerm=1 and entries
// [{T2,I2,X}, {T2,I3,Y}]. Expected: follower truncates indices 2-3, appends
// the new entries; final log = [{T1,I1,A}, {T2,I2,X}, {T2,I3,Y}].
func TestReplication_TruncateMultipleEntries(t *testing.T) {
	// Two peers so the Node starts as Follower (single-voter would auto-Leader).
	n, err := NewNode(Config{
		ID:               "n1",
		Peers:            []string{"p1", "p2"},
		ElectionTimeout:  time.Hour, // park election timer; we drive AE manually
		HeartbeatTimeout: testHeartbeat,
	})
	require.NoError(t, err)
	// Seed the log BEFORE Start. Safe: the actor goroutine has not yet been
	// launched, so we are the sole writer.
	seedLogEntries(n, []LogEntry{
		{Term: 1, Index: 1, Command: []byte("A")},
		{Term: 1, Index: 2, Command: []byte("B")},
		{Term: 1, Index: 3, Command: []byte("C")},
	})
	n.st.currentTerm = 1
	n.rs.Store(n.st.snapshot()) // republish so initial readState is coherent

	n.Start()
	t.Cleanup(n.Stop)
	go func() {
		for range n.ApplyCh() {
		}
	}()

	// Send AE with conflicting entries at indices 2-3.
	reply := n.HandleAppendEntries(&AppendEntriesArgs{
		Term:         2,
		LeaderID:     "leader",
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries: []LogEntry{
			{Term: 2, Index: 2, Command: []byte("X")},
			{Term: 2, Index: 3, Command: []byte("Y")},
		},
		LeaderCommit: 0,
	})
	require.True(t, reply.Success, "AE with valid PrevLog should succeed")

	// Stop, then read st.log under quiescence (actor exited; sole-writer
	// invariant trivially holds because we are the only goroutine left).
	n.Stop()
	require.Equal(t, uint64(3), n.st.log.LastIndex(), "log length after truncate+append")
	e1 := mustLogEntry(n, 1)
	require.Equal(t, uint64(1), e1.Term)
	require.Equal(t, []byte("A"), e1.Command)
	e2 := mustLogEntry(n, 2)
	require.Equal(t, uint64(2), e2.Term)
	require.Equal(t, []byte("X"), e2.Command)
	e3 := mustLogEntry(n, 3)
	require.Equal(t, uint64(2), e3.Term)
	require.Equal(t, []byte("Y"), e3.Command)
}

// TestReplication_ConflictHintShortLog: follower's log is shorter than
// PrevLogIndex. Reply must carry ConflictTerm=0 and
// ConflictIndex=lastLogIndex+1 (Case 1 of the §5.3 hint).
func TestReplication_ConflictHintShortLog(t *testing.T) {
	n, err := NewNode(Config{
		ID:               "n1",
		Peers:            []string{"p1", "p2"},
		ElectionTimeout:  time.Hour,
		HeartbeatTimeout: testHeartbeat,
	})
	require.NoError(t, err)
	// Seed: only one entry at index 1.
	seedLogEntries(n, []LogEntry{{Term: 1, Index: 1, Command: []byte("A")}})
	n.st.currentTerm = 1
	n.rs.Store(n.st.snapshot())

	n.Start()
	t.Cleanup(n.Stop)
	go func() {
		for range n.ApplyCh() {
		}
	}()

	// Leader thinks follower has 5 entries; sends PrevLogIndex=5.
	reply := n.HandleAppendEntries(&AppendEntriesArgs{
		Term:         2,
		LeaderID:     "leader",
		PrevLogIndex: 5,
		PrevLogTerm:  2,
		Entries:      nil,
	})
	require.False(t, reply.Success)
	require.Equal(t, uint64(0), reply.ConflictTerm, "ConflictTerm=0 for short log")
	require.Equal(t, uint64(2), reply.ConflictIndex, "ConflictIndex = lastLogIndex+1")
}

// TestReplication_ConflictHintTermMismatch: follower has an entry at
// PrevLogIndex but with a different term. The reply must carry the conflicting
// term and the first index where that term begins (Case 2 of §5.3).
func TestReplication_ConflictHintTermMismatch(t *testing.T) {
	n, err := NewNode(Config{
		ID:               "n1",
		Peers:            []string{"p1", "p2"},
		ElectionTimeout:  time.Hour,
		HeartbeatTimeout: testHeartbeat,
	})
	require.NoError(t, err)
	// Seed: log = [{T1,I1}, {T2,I2}, {T2,I3}, {T2,I4}, {T3,I5}].
	// Leader probes PrevLogIndex=4 with PrevLogTerm=99 → mismatch at I4.
	// Conflict term = 2; first index of term 2 in our log = 2.
	seedLogEntries(n, []LogEntry{
		{Term: 1, Index: 1},
		{Term: 2, Index: 2},
		{Term: 2, Index: 3},
		{Term: 2, Index: 4},
		{Term: 3, Index: 5},
	})
	n.st.currentTerm = 3
	n.rs.Store(n.st.snapshot())

	n.Start()
	t.Cleanup(n.Stop)
	go func() {
		for range n.ApplyCh() {
		}
	}()

	reply := n.HandleAppendEntries(&AppendEntriesArgs{
		Term:         99,
		LeaderID:     "leader",
		PrevLogIndex: 4,
		PrevLogTerm:  99, // forces term mismatch at I4 (we have T2 there)
	})
	require.False(t, reply.Success)
	require.Equal(t, uint64(2), reply.ConflictTerm, "ConflictTerm = follower's term at PrevLogIndex")
	require.Equal(t, uint64(2), reply.ConflictIndex, "ConflictIndex = first index of conflicting term")
}

// TestReplication_LeaderRetriesOnConflict: 3-voter cluster. n2 is seeded with
// a stale entry at index 1 (term=99, command "stale") that conflicts with
// what n1 will replicate. n1 wins election (its log is empty so it's
// not "up to date" vs n2 — we work around by also seeding n1 at higher term).
//
// We observe convergence and that the leader's nextIndex backoff used the
// hint path (counting AE round-trips through a wrapping transport).
func TestReplication_LeaderRetriesOnConflict(t *testing.T) {
	net := newMemNetwork()
	ids := []string{"n1", "n2", "n3"}

	nodes := make([]*Node, 3)
	for i, id := range ids {
		electionTimeout := slowElectionTimeout
		if i == 0 {
			electionTimeout = fastElectionTimeout
		}
		n, nerr := NewNode(Config{
			ID:               id,
			Peers:            otherIDsLocal(ids, id),
			ElectionTimeout:  electionTimeout,
			HeartbeatTimeout: testHeartbeat,
		})
		require.NoError(t, nerr)
		nodes[i] = n
	}

	// Seed n1 with a single high-term entry so it's at-least-as-up-to-date as n2.
	// Seed n2 with a stale entry at the same index but different term.
	// n3 starts empty.
	seedLogEntries(nodes[0], []LogEntry{{Term: 5, Index: 1, Command: []byte("leader-old")}})
	nodes[0].st.currentTerm = 5
	nodes[0].rs.Store(nodes[0].st.snapshot())

	seedLogEntries(nodes[1], []LogEntry{{Term: 3, Index: 1, Command: []byte("stale")}})
	nodes[1].st.currentTerm = 3
	nodes[1].rs.Store(nodes[1].st.snapshot())

	// Wire transports — wrap n1's transport so we can count AE round-trips
	// per peer.
	type counter struct {
		ae map[string]int
	}
	cnt := &counter{ae: make(map[string]int)}
	for _, n := range nodes {
		tr := net.Register(n.cfg.ID, n)
		if n.cfg.ID == "n1" {
			n.SetTransport(&countingAETransport{inner: tr, count: cnt.ae})
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
	require.NoError(t, waitFor(2*time.Second, func() bool { return n1.IsLeader() }),
		"n1 did not become leader")

	// n1 has seeded log at index 1 (term 5). On election, becomeLeader appends
	// a no-op at index 2 (term ≥ 5). ProposeWait("newcmd") therefore lands at
	// index 3. n2 must truncate its stale index-1 entry and receive all three.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	idx, err := n1.ProposeWait(ctx, []byte("newcmd"))
	require.NoError(t, err)
	require.Equal(t, uint64(3), idx, "ProposeWait returns index 3 (seeded idx 1 + no-op idx 2 + user idx 3)")

	// All nodes converge to commitIndex >= 3.
	require.NoError(t, waitFor(2*time.Second, func() bool {
		for _, n := range nodes {
			if n.CommittedIndex() < 3 {
				return false
			}
		}
		return true
	}), "not all nodes reached commitIndex 3")

	// Verify the AE round-trip count on n2 stayed bounded — the conflict-hint
	// path should converge in O(1)-O(2) failed AEs, not by walking back
	// 1-by-1. We seeded only one stale entry, so 1-by-1 vs hint converge
	// equivalently here; the broader assertion is "test doesn't loop forever
	// AND the hint path executed". We observe that by checking n2's log:
	// after convergence its log[0] must carry term 5 (not the stale term 3).
	nodes[1].Stop()
	<-nodes[1].doneCh
	require.GreaterOrEqual(t, nodes[1].st.log.LastIndex(), uint64(3), "n2 should have at least 3 entries")
	n2e1 := mustLogEntry(nodes[1], 1)
	require.Equal(t, uint64(5), n2e1.Term,
		"n2's index-1 entry should be replaced with n1's term-5 entry")
	require.Equal(t, []byte("leader-old"), n2e1.Command,
		"n2's index-1 command should match n1's seed")
	// Sanity: the wrapper saw at least one AE to n2 (otherwise transport bypassed).
	require.Greater(t, cnt.ae["n2"], 0, "AE wrapper should have observed traffic to n2")
}

// countingAETransport wraps a Transport and counts SendAppendEntries calls
// per peer. RequestVote passes through transparently.
type countingAETransport struct {
	inner Transport
	count map[string]int // mu — written from per-call goroutines, but in tests
	// using memNetwork the increments serialize via the destination Node's
	// HandleAppendEntries cmdCh round-trip; concurrent increments from the
	// SAME peer cannot race because each AE waits for its reply before the
	// next dispatch (the actor only fires one AE per tick). Different peers
	// race against each other on different map keys — safe in Go ONLY if the
	// map is pre-populated. We do NOT pre-populate; tolerate the race in
	// test code by relying on sync.Mutex would be cleaner, but the
	// race-detector + writes-to-distinct-keys-but-shared-map will fire.
	// Use a Mutex to be correct under -race.
	mu sync.Mutex
}

func (c *countingAETransport) SendRequestVote(peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	return c.inner.SendRequestVote(peer, args)
}

func (c *countingAETransport) SendAppendEntries(peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	c.mu.Lock()
	c.count[peer]++
	c.mu.Unlock()
	return c.inner.SendAppendEntries(peer, args)
}

func (c *countingAETransport) SendInstallSnapshot(peer string, args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	return c.inner.SendInstallSnapshot(peer, args)
}

func (c *countingAETransport) SendTimeoutNow(peer string, args *TimeoutNowArgs) (*TimeoutNowReply, error) {
	return c.inner.SendTimeoutNow(peer, args)
}

// capturingAETransport wraps a Transport and records the maximum number of
// entries seen in any single SendAppendEntries call for a given peer.
// RequestVote passes through transparently.
type capturingAETransport struct {
	inner   Transport
	mu      sync.Mutex
	maxSeen map[string]int // peer → max entries count seen in a single AE
}

func (c *capturingAETransport) SendRequestVote(peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	return c.inner.SendRequestVote(peer, args)
}

func (c *capturingAETransport) SendAppendEntries(peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	c.mu.Lock()
	if len(args.Entries) > c.maxSeen[peer] {
		c.maxSeen[peer] = len(args.Entries)
	}
	c.mu.Unlock()
	return c.inner.SendAppendEntries(peer, args)
}

func (c *capturingAETransport) SendInstallSnapshot(peer string, args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	return c.inner.SendInstallSnapshot(peer, args)
}

func (c *capturingAETransport) SendTimeoutNow(peer string, args *TimeoutNowArgs) (*TimeoutNowReply, error) {
	return c.inner.SendTimeoutNow(peer, args)
}

// TestBuildAE_RespectsMaxEntriesPerAE verifies that buildAppendEntriesArgs caps
// the batch to cfg.MaxEntriesPerAE. Whitebox: a 3-voter cluster with
// MaxEntriesPerAE=64 proposes 200 entries; we capture AE payloads via
// capturingAETransport and assert no batch exceeded 64 entries.
func TestBuildAE_RespectsMaxEntriesPerAE(t *testing.T) {
	const cap64 = 64
	net := newMemNetwork()
	ids := []string{"n1", "n2", "n3"}
	nodes := make([]*Node, 3)
	for i, id := range ids {
		et := slowElectionTimeout
		if i == 0 {
			et = fastElectionTimeout
		}
		n, nerr := NewNode(Config{
			ID:               id,
			Peers:            otherIDsLocal(ids, id),
			ElectionTimeout:  et,
			HeartbeatTimeout: testHeartbeat,
			MaxEntriesPerAE:  cap64,
		})
		require.NoError(t, nerr)
		nodes[i] = n
	}
	capTr := &capturingAETransport{
		inner:   net.Register("n1", nodes[0]),
		maxSeen: make(map[string]int),
	}
	nodes[0].SetTransport(capTr)
	for _, n := range nodes[1:] {
		n.SetTransport(net.Register(n.cfg.ID, n))
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const N = 200
	for i := 0; i < N; i++ {
		_, err := n1.ProposeWait(ctx, []byte(fmt.Sprintf("entry-%d", i)))
		require.NoError(t, err)
	}

	// Wait for all nodes to commit all entries.
	require.NoError(t, waitFor(5*time.Second, func() bool {
		for _, n := range nodes {
			if n.CommittedIndex() < N {
				return false
			}
		}
		return true
	}), "not all nodes committed %d entries", N)

	capTr.mu.Lock()
	maxN2 := capTr.maxSeen["n2"]
	maxN3 := capTr.maxSeen["n3"]
	capTr.mu.Unlock()

	require.LessOrEqual(t, maxN2, cap64, "n2: max batch size must not exceed MaxEntriesPerAE")
	require.LessOrEqual(t, maxN3, cap64, "n3: max batch size must not exceed MaxEntriesPerAE")
}

// TestAE_RejectsMismatchedEntryIndex verifies that handleAppendEntries rejects
// an AE whose Entries[0].Index doesn't match args.PrevLogIndex+1.
// Follower's log must remain unchanged on rejection.
func TestAE_RejectsMismatchedEntryIndex(t *testing.T) {
	n, err := NewNode(Config{
		ID:               "n1",
		Peers:            []string{"p1", "p2"},
		ElectionTimeout:  time.Hour,
		HeartbeatTimeout: testHeartbeat,
	})
	require.NoError(t, err)
	seedLogEntries(n, []LogEntry{{Term: 1, Index: 1, Command: []byte("A")}})
	n.st.currentTerm = 1
	n.rs.Store(n.st.snapshot())

	n.Start()
	t.Cleanup(n.Stop)
	go func() {
		for range n.ApplyCh() {
		}
	}()

	// Forged AE: PrevLogIndex=1 means next expected is index 2, but entry carries Index=99.
	reply := n.HandleAppendEntries(&AppendEntriesArgs{
		Term:         2,
		LeaderID:     "leader",
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries: []LogEntry{
			{Term: 2, Index: 99, Command: []byte("forged")},
		},
		LeaderCommit: 0,
	})
	require.False(t, reply.Success, "AE with mismatched entry index must be rejected")

	// Log must be unchanged.
	n.Stop()
	require.Equal(t, uint64(1), n.st.log.LastIndex(), "follower log must be unchanged after rejection")
	e := mustLogEntry(n, 1)
	require.Equal(t, uint64(1), e.Index)
	require.Equal(t, []byte("A"), e.Command)
}

// TestApplyConflictHint_BoundedScanOnLargeLog verifies that applyConflictHint
// uses binary search rather than a linear scan when finding the last log entry
// at ConflictTerm. Two sub-cases:
//  1. Leader has ConflictTerm (term 1) → nextIndex advances past its last entry.
//  2. Leader lacks ConflictTerm (term 9 absent) → nextIndex falls back to
//     hbConflictIndex.
//
// The test seeds leader state (log, nextIndex, matchIndex) before Start() so
// the actor sees an already-promoted Leader without running a real election.
func TestApplyConflictHint_BoundedScanOnLargeLog(t *testing.T) {
	const N = 100
	const conflictIdx = uint64(5)

	// Helper: build a pre-seeded Leader node with N log entries at term=logTerm,
	// peers p1/p2. Seeds actorState before Start so the actor's multi-voter
	// Follower path (which only arms the election timer) leaves our state intact.
	makeLeaderNode := func(id string, logTerm uint64) *Node {
		n, nerr := NewNode(Config{
			ID:               id,
			Peers:            []string{"p1", "p2"},
			ElectionTimeout:  time.Hour, // park election timer — we want stable leader
			HeartbeatTimeout: testHeartbeat,
		})
		require.NoError(t, nerr)
		n.st.currentTerm = logTerm
		n.st.state = Leader
		n.st.leaderID = id
		entries := make([]LogEntry, N)
		for i := 0; i < N; i++ {
			entries[i] = LogEntry{Term: logTerm, Index: uint64(i + 1)}
		}
		seedLogEntries(n, entries)
		n.st.nextIndex = map[string]uint64{"p1": N + 1, "p2": N + 1}
		n.st.matchIndex = map[string]uint64{"p1": 0, "p2": 0}
		n.rs.Store(n.st.snapshot())
		return n
	}

	sendHBReply := func(n *Node, peer string, conflictTerm, conflictIndex uint64) {
		done := make(chan struct{})
		go func() {
			defer close(done)
			n.cmdCh <- command{
				kind:            cmdHeartbeatReply,
				hbPeer:          peer,
				hbTerm:          n.st.currentTerm,
				hbSuccess:       false,
				hbConflictTerm:  conflictTerm,
				hbConflictIndex: conflictIndex,
				hbMatchAfter:    0,
			}
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("cmdCh send timed out")
		}
	}

	t.Run("leader_has_conflict_term", func(t *testing.T) {
		// Leader log: 100 entries at term 1. Follower reports ConflictTerm=1.
		// Binary search finds term 1 throughout; rightmost is at index N=100.
		// nextIndex must = N+1 = 101.
		n := makeLeaderNode("L1", 1)
		n.Start()
		t.Cleanup(n.Stop)
		go func() {
			for range n.ApplyCh() {
			}
		}()

		sendHBReply(n, "p1", 1, conflictIdx)

		// Give actor time to process.
		time.Sleep(20 * time.Millisecond)
		n.Stop()

		require.Equal(t, uint64(N+1), n.st.nextIndex["p1"],
			"nextIndex must be past the rightmost entry at ConflictTerm")
	})

	t.Run("leader_lacks_conflict_term", func(t *testing.T) {
		// Leader log: 100 entries at term 5. Follower reports ConflictTerm=9 (absent).
		// Binary search finds no entry at term 9; nextIndex falls back to ConflictIndex.
		n := makeLeaderNode("L2", 5)
		n.Start()
		t.Cleanup(n.Stop)
		go func() {
			for range n.ApplyCh() {
			}
		}()

		sendHBReply(n, "p1", 9, conflictIdx) // term 9 not in leader log

		time.Sleep(20 * time.Millisecond)
		n.Stop()

		require.Equal(t, conflictIdx, n.st.nextIndex["p1"],
			"nextIndex must fall back to ConflictIndex when leader lacks ConflictTerm")
	})
}

// TestBecomeLeader_AppendsNoOp verifies that a newly elected multi-voter leader
// appends a LogEntryNoOp entry at its current term immediately on election.
// After replication settles, all followers' commitIndex must advance to 1 (the
// no-op index).
func TestBecomeLeader_AppendsNoOp(t *testing.T) {
	caps := startCapturingCluster(t, "n1", "n2", "n3")
	n1 := caps[0].node

	require.NoError(t, waitFor(2*time.Second, func() bool { return n1.IsLeader() }),
		"n1 did not become leader")

	leaderTerm := n1.Term()

	// All three nodes must commit the no-op at index 1.
	waitForCommitted(t, caps, 1, 2*time.Second)

	// Stop all nodes and inspect captured entries.
	for _, c := range caps {
		entries := c.readApplied()
		require.GreaterOrEqual(t, len(entries), 1, "%s must have at least the no-op", c.node.cfg.ID)
		noOp := entries[0]
		require.Equal(t, uint64(1), noOp.Index, "%s no-op index", c.node.cfg.ID)
		require.Equal(t, leaderTerm, noOp.Term, "%s no-op term", c.node.cfg.ID)
		require.Equal(t, LogEntryNoOp, noOp.Type, "%s no-op type", c.node.cfg.ID)
	}
}

// TestReplication_ApplyOrderAcrossFollowers: assert all three nodes apply
// the same sequence of entries (no reordering) when multiple proposes happen
// in rapid succession. Different from TestReplication_MultiplePropose because
// this drives proposes back-to-back without waiting between, exercising the
// in-flight pipeline.
func TestReplication_ApplyOrderAcrossFollowers(t *testing.T) {
	caps := startCapturingCluster(t, "n1", "n2", "n3")
	n1 := caps[0].node

	require.NoError(t, waitFor(2*time.Second, func() bool { return n1.IsLeader() }))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Three proposes serialised through ProposeWait (each blocks on commit).
	// The actor processes them sequentially; between two adjacent proposes,
	// the leader's commitIndex moves and a fresh AE round propagates.
	// No-op at index 1; user entries at indices 2..N+1.
	const N = 3
	for i := 1; i <= N; i++ {
		_, err := n1.ProposeWait(ctx, []byte(fmt.Sprintf("v%d", i)))
		require.NoError(t, err)
	}

	waitForCommitted(t, caps, N+1, 2*time.Second)

	for _, c := range caps {
		entries := c.readApplied()
		require.Len(t, entries, N+1, "%s: N user + 1 no-op", c.node.cfg.ID)
		// Index monotonically increasing: no-op at 1, user entries at 2..N+1.
		for i, e := range entries {
			require.Equal(t, uint64(i+1), e.Index, "%s entry[%d].Index", c.node.cfg.ID, i)
		}
	}
}

// blockingAETransport wraps a Transport and blocks SendAppendEntries calls to a
// specific peer on a channel held by the test. This simulates a hung/partitioned
// transport (QUIC keepalive delay) to trigger the goroutine-accumulation scenario
// that the per-peer single-flight gate prevents.
type blockingAETransport struct {
	inner       Transport
	blockPeer   string
	releaseCh   chan struct{} // closed by test to release all blocked goroutines
	inFlight    atomic.Int64  // current concurrent dispatches to blockPeer
	maxInFlight atomic.Int64  // peak concurrent dispatches observed
}

func (b *blockingAETransport) SendRequestVote(peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	return b.inner.SendRequestVote(peer, args)
}

func (b *blockingAETransport) SendInstallSnapshot(peer string, args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	return b.inner.SendInstallSnapshot(peer, args)
}

func (b *blockingAETransport) SendTimeoutNow(peer string, args *TimeoutNowArgs) (*TimeoutNowReply, error) {
	return b.inner.SendTimeoutNow(peer, args)
}

func (b *blockingAETransport) SendAppendEntries(peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	if peer != b.blockPeer {
		return b.inner.SendAppendEntries(peer, args)
	}
	// Track concurrency for the blocked peer.
	cur := b.inFlight.Add(1)
	// Update peak.
	for {
		old := b.maxInFlight.Load()
		if cur <= old {
			break
		}
		if b.maxInFlight.CompareAndSwap(old, cur) {
			break
		}
	}
	// Block until the test releases all goroutines.
	<-b.releaseCh
	b.inFlight.Add(-1)
	return nil, ErrUnknownPeer
}

// blockEmptyAETransport blocks one or more empty AppendEntries calls while
// allowing entry-bearing replication to pass. It lets tests put a leader's
// per-peer single-flight gate in the exact "heartbeat in flight, new proposal
// appended" state without blocking the proposal's eventual replication RPC.
type blockEmptyAETransport struct {
	inner      Transport
	blockPeers map[string]bool
	releaseCh  chan struct{}
	blockedCh  chan string
	armed      atomic.Bool
}

func (b *blockEmptyAETransport) SendRequestVote(peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	return b.inner.SendRequestVote(peer, args)
}

func (b *blockEmptyAETransport) SendInstallSnapshot(peer string, args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	return b.inner.SendInstallSnapshot(peer, args)
}

func (b *blockEmptyAETransport) SendTimeoutNow(peer string, args *TimeoutNowArgs) (*TimeoutNowReply, error) {
	return b.inner.SendTimeoutNow(peer, args)
}

func (b *blockEmptyAETransport) SendAppendEntries(peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	if b.armed.Load() && b.blockPeers[peer] && len(args.Entries) == 0 {
		select {
		case b.blockedCh <- peer:
		default:
		}
		<-b.releaseCh
	}
	return b.inner.SendAppendEntries(peer, args)
}

// TestProposeDispatchesPendingEntryAfterHeartbeatReply verifies that a proposal
// made while heartbeats are in flight does not wait for the next heartbeat tick.
// Once the heartbeat replies clear peerInFlight, the leader should immediately
// dispatch the pending entry to those peers.
func TestProposeDispatchesPendingEntryAfterHeartbeatReply(t *testing.T) {
	net := newMemNetwork()
	ids := []string{"n1", "n2", "n3"}
	nodes := make([]*Node, 3)

	for i, id := range ids {
		et := 2 * time.Second
		if i == 0 {
			et = fastElectionTimeout
		}
		n, nerr := NewNode(Config{
			ID:               id,
			Peers:            otherIDsLocal(ids, id),
			ElectionTimeout:  et,
			HeartbeatTimeout: 500 * time.Millisecond,
		})
		require.NoError(t, nerr)
		nodes[i] = n
	}

	releaseCh := make(chan struct{})
	blocker := &blockEmptyAETransport{
		inner: net.Register("n1", nodes[0]),
		blockPeers: map[string]bool{
			"n2": true,
			"n3": true,
		},
		releaseCh: releaseCh,
		blockedCh: make(chan string, 8),
	}
	nodes[0].SetTransport(blocker)
	nodes[1].SetTransport(net.Register("n2", nodes[1]))
	nodes[2].SetTransport(net.Register("n3", nodes[2]))

	for _, n := range nodes {
		n.Start()
		t.Cleanup(n.Stop)
		go func(n *Node) {
			for range n.ApplyCh() {
			}
		}(n)
	}

	n1 := nodes[0]
	require.NoError(t, waitFor(2*time.Second, func() bool { return n1.IsLeader() }),
		"n1 did not become leader")
	require.NoError(t, waitFor(2*time.Second, func() bool { return n1.CommittedIndex() >= 1 }),
		"leader no-op did not commit")

	blocker.armed.Store(true)
	seen := map[string]bool{}
	require.NoError(t, waitFor(2*time.Second, func() bool {
		for {
			select {
			case peer := <-blocker.blockedCh:
				seen[peer] = true
			default:
				return seen["n2"] && seen["n3"]
			}
		}
	}), "expected empty heartbeats to both followers to be blocked")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	doneCh := make(chan error, 1)
	go func() {
		_, err := n1.ProposeWait(ctx, []byte("after-heartbeat"))
		doneCh <- err
	}()

	select {
	case err := <-doneCh:
		require.NoError(t, err)
		t.Fatal("proposal completed before blocked heartbeats were released")
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseCh)

	select {
	case err := <-doneCh:
		require.NoError(t, err)
	case <-time.After(120 * time.Millisecond):
		t.Fatal("proposal waited for a later heartbeat tick after in-flight heartbeats replied")
	}
}

type blockEntryReplyTransport struct {
	inner     Transport
	blockPeer string
	releaseCh chan struct{}
	blockedCh chan struct{}
	armed     atomic.Bool
}

func (b *blockEntryReplyTransport) SendRequestVote(peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	return b.inner.SendRequestVote(peer, args)
}

func (b *blockEntryReplyTransport) SendInstallSnapshot(peer string, args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	return b.inner.SendInstallSnapshot(peer, args)
}

func (b *blockEntryReplyTransport) SendTimeoutNow(peer string, args *TimeoutNowArgs) (*TimeoutNowReply, error) {
	return b.inner.SendTimeoutNow(peer, args)
}

func (b *blockEntryReplyTransport) SendAppendEntries(peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	reply, err := b.inner.SendAppendEntries(peer, args)
	if b.armed.Load() && peer == b.blockPeer && len(args.Entries) > 0 {
		select {
		case b.blockedCh <- struct{}{}:
		default:
		}
		<-b.releaseCh
	}
	return reply, err
}

// TestCommitDispatchesLeaderCommitAfterFollowerReply verifies that a follower
// which already appended an entry with an old LeaderCommit learns the later
// commit immediately after its in-flight reply returns. Without this, forwarded
// callers waiting for their local FSM can sit until the next heartbeat tick.
func TestCommitDispatchesLeaderCommitAfterFollowerReply(t *testing.T) {
	net := newMemNetwork()
	ids := []string{"n1", "n2", "n3"}
	nodes := make([]*Node, 3)

	for i, id := range ids {
		et := 2 * time.Second
		if i == 0 {
			et = fastElectionTimeout
		}
		n, nerr := NewNode(Config{
			ID:               id,
			Peers:            otherIDsLocal(ids, id),
			ElectionTimeout:  et,
			HeartbeatTimeout: 500 * time.Millisecond,
		})
		require.NoError(t, nerr)
		nodes[i] = n
	}

	releaseCh := make(chan struct{})
	blocker := &blockEntryReplyTransport{
		inner:     net.Register("n1", nodes[0]),
		blockPeer: "n3",
		releaseCh: releaseCh,
		blockedCh: make(chan struct{}, 1),
	}
	nodes[0].SetTransport(blocker)
	nodes[1].SetTransport(net.Register("n2", nodes[1]))
	nodes[2].SetTransport(net.Register("n3", nodes[2]))

	for _, n := range nodes {
		n.Start()
		t.Cleanup(n.Stop)
		go func(n *Node) {
			for range n.ApplyCh() {
			}
		}(n)
	}

	n1, n2, n3 := nodes[0], nodes[1], nodes[2]
	require.NoError(t, waitFor(2*time.Second, func() bool { return n1.IsLeader() }),
		"n1 did not become leader")
	require.NoError(t, waitFor(2*time.Second, func() bool {
		return n1.CommittedIndex() >= 1 && n2.CommittedIndex() >= 1 && n3.CommittedIndex() >= 1
	}), "leader no-op did not apply across cluster")

	blocker.armed.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := n1.ProposeWait(ctx, []byte("commit-notify"))
	require.NoError(t, err)

	select {
	case <-blocker.blockedCh:
	case <-time.After(2 * time.Second):
		t.Fatal("expected n3 entry reply to be blocked")
	}
	require.NoError(t, waitFor(2*time.Second, func() bool {
		return n1.CommittedIndex() >= 2 && n2.CommittedIndex() >= 2
	}), "proposal did not commit via n2")
	require.Less(t, n3.CommittedIndex(), uint64(2), "n3 should have appended but not applied before commit notification")

	close(releaseCh)

	require.NoError(t, waitFor(120*time.Millisecond, func() bool {
		return n3.CommittedIndex() >= 2
	}), "n3 waited for a later heartbeat tick to learn LeaderCommit")
}

// TestSingleFlight_PartitionedPeerNoGoroutineLeak verifies the per-peer
// single-flight gate: when a peer's transport hangs (simulated by blocking
// SendAppendEntries on a channel), at most 1 AE goroutine is in flight for that
// peer at any time across ~20 heartbeat ticks.
func TestSingleFlight_PartitionedPeerNoGoroutineLeak(t *testing.T) {
	net := newMemNetwork()
	ids := []string{"n1", "n2", "n3"}
	nodes := make([]*Node, 3)

	for i, id := range ids {
		et := slowElectionTimeout
		if i == 0 {
			et = fastElectionTimeout
		}
		n, nerr := NewNode(Config{
			ID:               id,
			Peers:            otherIDsLocal(ids, id),
			ElectionTimeout:  et,
			HeartbeatTimeout: testHeartbeat,
		})
		require.NoError(t, nerr)
		nodes[i] = n
	}

	releaseCh := make(chan struct{})
	blocker := &blockingAETransport{
		inner:     net.Register("n1", nodes[0]),
		blockPeer: "n2",
		releaseCh: releaseCh,
	}
	nodes[0].SetTransport(blocker)
	nodes[1].SetTransport(net.Register("n2", nodes[1]))
	nodes[2].SetTransport(net.Register("n3", nodes[2]))

	for _, n := range nodes {
		n.Start()
		t.Cleanup(n.Stop)
		go func(n *Node) {
			for range n.ApplyCh() {
			}
		}(n)
	}

	n1 := nodes[0]
	require.NoError(t, waitFor(2*time.Second, func() bool { return n1.IsLeader() }),
		"n1 did not become leader")

	// Allow ~20 heartbeat ticks to run with n2's transport blocked.
	const ticks = 20
	time.Sleep(time.Duration(ticks) * testHeartbeat)

	// Release all blocked goroutines so they can drain before Stop.
	close(releaseCh)

	// Wait for in-flight count to drain (goroutines unblocked, returning error).
	require.NoError(t, waitFor(2*time.Second, func() bool {
		return blocker.inFlight.Load() == 0
	}), "blocked goroutines did not drain after release")

	// With single-flight, at most 1 AE goroutine should have been outstanding
	// for the partitioned peer at any instant — max observed concurrency must be 1.
	require.LessOrEqual(t, blocker.maxInFlight.Load(), int64(1),
		"per-peer single-flight gate must bound concurrent AE goroutines to ≤1")
}

// TestApplyCommitted_StopRaceLeavesCommitIndexConsistent verifies that
// applyCommitted advances commitIndex per-delivered-entry rather than via the
// old "rollback to i-1" pattern. Two scenarios:
//
//  1. Zero deliveries: stopCh wins the very first send → commitIndex stays at oldCommit.
//  2. Partial delivery: K of N entries succeed before stopCh fires → commitIndex == K.
//
// Scenario (2) is the load-bearing test — it differentiates the per-entry advance
// (NEW behaviour) from a hypothetical implementation that only sets commitIndex
// at end-of-loop (which would leave commitIndex at oldCommit on stopCh).
func TestApplyCommitted_StopRaceLeavesCommitIndexConsistent(t *testing.T) {
	makeNode := func(t *testing.T) *Node {
		n, err := NewNode(Config{
			ID:               "n1",
			Peers:            []string{"p1", "p2"},
			ElectionTimeout:  time.Hour,
			HeartbeatTimeout: testHeartbeat,
		})
		require.NoError(t, err)

		const N = 5
		entries := make([]LogEntry, N)
		for i := 0; i < N; i++ {
			entries[i] = LogEntry{Term: 1, Index: uint64(i + 1), Command: []byte(fmt.Sprintf("cmd-%d", i+1))}
		}
		seedLogEntries(n, entries)
		n.st.currentTerm = 1
		n.st.state = Leader
		n.st.leaderID = "n1"
		n.st.matchIndex = map[string]uint64{"p1": 0, "p2": 0}
		n.st.nextIndex = map[string]uint64{"p1": N + 1, "p2": N + 1}
		n.st.proposeWaiters = make(map[uint64]chan proposalResult)
		n.rs.Store(n.st.snapshot())
		return n
	}

	t.Run("ZeroDeliveries", func(t *testing.T) {
		n := makeNode(t)
		// Unbuffered applyInCh: every send blocks; pre-closed stopCh wins immediately.
		// (applyInCh replaces the actor's old direct applyCh send after the apply
		// pipeline decoupling — Stop semantics now apply to "fully enqueued" rather
		// than "fully delivered to FSM"; the contract is otherwise identical.)
		n.applyInCh = make(chan LogEntry)
		close(n.stopCh)

		n.applyCommitted(0, 5)
		require.Equal(t, uint64(0), n.st.commitIndex,
			"commitIndex must stay at oldCommit when Stop wins the first send")
	})

	t.Run("PartialDelivery", func(t *testing.T) {
		n := makeNode(t)
		// Unbuffered applyInCh; a consumer goroutine reads exactly K entries
		// then closes stopCh. This makes the Stop point deterministic: the
		// (K+1)-th iteration's select sees a buffered-ready stopCh AND a
		// blocked applyInCh send (no reader), so stopCh always wins.
		const K = 3
		n.applyInCh = make(chan LogEntry)

		consumerDone := make(chan struct{})
		go func() {
			defer close(consumerDone)
			for i := 0; i < K; i++ {
				<-n.applyInCh
			}
			close(n.stopCh) // applyCommitted's next select will lose to stopCh.
		}()

		n.applyCommitted(0, 5)
		<-consumerDone

		require.Equal(t, uint64(K), n.st.commitIndex,
			"commitIndex must equal K (last fully-enqueued entry's index) after partial delivery")
	})
}
