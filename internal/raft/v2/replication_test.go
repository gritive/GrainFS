package raftv2

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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
		n := NewNode(Config{
			ID:               id,
			Peers:            peers,
			ElectionTimeout:  electionTimeout,
			HeartbeatTimeout: testHeartbeat,
		})
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

	idx, err := n1.ProposeWait(ctx, []byte("hello"))
	require.NoError(t, err)
	require.Equal(t, uint64(1), idx, "first ProposeWait should return index 1")

	// All three nodes must reach commitIndex >= 1.
	waitForCommitted(t, caps, 1, 2*time.Second)

	// Drain by stopping nodes; verify each captured exactly the proposed entry.
	for _, c := range caps {
		entries := c.readApplied()
		require.Len(t, entries, 1, "%s applied count", c.node.cfg.ID)
		require.Equal(t, uint64(1), entries[0].Index, "%s entry index", c.node.cfg.ID)
		require.Equal(t, []byte("hello"), entries[0].Command, "%s entry command", c.node.cfg.ID)
		// Term comes from the leader at propose time; since n1 won term 1
		// uncontested, the entry must carry term 1.
		require.Equal(t, uint64(1), entries[0].Term, "%s entry term", c.node.cfg.ID)
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

	const N = 5
	for i := 1; i <= N; i++ {
		cmd := []byte(fmt.Sprintf("cmd-%d", i))
		idx, err := n1.ProposeWait(ctx, cmd)
		require.NoError(t, err)
		require.Equal(t, uint64(i), idx, "ProposeWait #%d", i)
	}

	waitForCommitted(t, caps, N, 2*time.Second)

	for _, c := range caps {
		entries := c.readApplied()
		require.Len(t, entries, N, "%s applied count", c.node.cfg.ID)
		for i, e := range entries {
			require.Equal(t, uint64(i+1), e.Index, "%s entry[%d].Index", c.node.cfg.ID, i)
			require.Equal(t, []byte(fmt.Sprintf("cmd-%d", i+1)), e.Command,
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
		nodes[i] = NewNode(Config{
			ID:               id,
			Peers:            peers,
			ElectionTimeout:  electionTimeout,
			HeartbeatTimeout: testHeartbeat,
		})
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
	const N = 3
	for i := 1; i <= N; i++ {
		_, err := n1.ProposeWait(ctx, []byte(fmt.Sprintf("v%d", i)))
		require.NoError(t, err)
	}

	waitForCommitted(t, caps, N, 2*time.Second)

	for _, c := range caps {
		entries := c.readApplied()
		require.Len(t, entries, N)
		for i, e := range entries {
			require.Equal(t, uint64(i+1), e.Index, "%s entry[%d].Index", c.node.cfg.ID, i)
		}
	}
}
