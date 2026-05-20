package raft

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
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

var _ = ginkgo.Describe("Replication scenarios", func() {
	startCapturedReplicationCluster := func(ids ...string) ([]*capturedNode, func(), error) {
		if len(ids) != 3 {
			return nil, nil, fmt.Errorf("startCapturedReplicationCluster expects exactly 3 ids")
		}

		net := newMemNetwork()
		captures := make([]*capturedNode, 0, len(ids))
		for i, id := range ids {
			electionTimeout := slowElectionTimeout
			if i == 0 {
				electionTimeout = fastElectionTimeout
			}
			node, err := NewNode(Config{
				ID:               id,
				Peers:            otherIDsLocal(ids, id),
				ElectionTimeout:  electionTimeout,
				HeartbeatTimeout: testHeartbeat,
			})
			if err != nil {
				return nil, nil, err
			}
			captures = append(captures, &capturedNode{node: node, doneCh: make(chan struct{})})
		}
		for _, capture := range captures {
			capture.node.SetTransport(net.Register(capture.node.cfg.ID, capture.node))
		}
		for _, capture := range captures {
			capture.node.Start()
			go func(capture *capturedNode) {
				defer close(capture.doneCh)
				for entry := range capture.node.ApplyCh() {
					capture.applied = append(capture.applied, entry)
				}
			}(capture)
		}

		stopped := false
		cleanup := func() {
			if stopped {
				return
			}
			stopped = true
			for _, capture := range captures {
				capture.node.Stop()
			}
			for _, capture := range captures {
				<-capture.doneCh
			}
		}
		return captures, cleanup, nil
	}

	waitForReplicationCommitted := func(captures []*capturedNode, idx uint64, timeout time.Duration) error {
		return waitFor(timeout, func() bool {
			for _, capture := range captures {
				if capture.node.CommittedIndex() < idx {
					return false
				}
			}
			return true
		})
	}

	startReplicationNodes := func(ids []string, configure func(int, string) Config, wire func(*memNetwork, []*Node)) ([]*Node, func(), error) {
		net := newMemNetwork()
		nodes := make([]*Node, len(ids))
		for i, id := range ids {
			node, err := NewNode(configure(i, id))
			if err != nil {
				return nil, nil, err
			}
			nodes[i] = node
		}
		wire(net, nodes)

		stopped := false
		cleanup := func() {
			if stopped {
				return
			}
			stopped = true
			for _, node := range nodes {
				node.Stop()
			}
		}
		for _, node := range nodes {
			node.Start()
			go func(node *Node) {
				for range node.ApplyCh() {
				}
			}(node)
		}
		return nodes, cleanup, nil
	}

	ginkgo.Context("captured three-voter cluster", func() {
		var captures []*capturedNode
		var cleanup func()
		var leader *Node

		ginkgo.BeforeEach(func() {
			var err error
			captures, cleanup, err = startCapturedReplicationCluster("n1", "n2", "n3")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.DeferCleanup(cleanup)
			leader = captures[0].node
			gomega.Expect(waitFor(2*time.Second, func() bool { return leader.IsLeader() })).To(gomega.Succeed(), "n1 did not become leader")
		})

		ginkgo.It("replicates one proposed entry to all voters", func(ginkgo.SpecContext) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			idx, err := leader.ProposeWait(ctx, []byte("hello"))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(idx).To(gomega.Equal(uint64(2)), "first user proposal follows the leader no-op")
			gomega.Expect(waitForReplicationCommitted(captures, 2, 2*time.Second)).To(gomega.Succeed())

			cleanup()
			for _, capture := range captures {
				entries := capture.applied
				gomega.Expect(entries).To(gomega.HaveLen(2), "%s applied count", capture.node.cfg.ID)
				gomega.Expect(entries[0].Index).To(gomega.Equal(uint64(1)), "%s no-op index", capture.node.cfg.ID)
				gomega.Expect(entries[0].Type).To(gomega.Equal(LogEntryNoOp), "%s no-op type", capture.node.cfg.ID)
				gomega.Expect(entries[1].Index).To(gomega.Equal(uint64(2)), "%s user entry index", capture.node.cfg.ID)
				gomega.Expect(entries[1].Command).To(gomega.Equal([]byte("hello")), "%s user entry command", capture.node.cfg.ID)
				gomega.Expect(entries[1].Term).To(gomega.Equal(uint64(1)), "%s user entry term", capture.node.cfg.ID)
			}
		}, ginkgo.NodeTimeout(5*time.Second))

		ginkgo.It("applies multiple proposals in submission order", func(ginkgo.SpecContext) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			const proposals = 5
			for i := 1; i <= proposals; i++ {
				idx, err := leader.ProposeWait(ctx, []byte(fmt.Sprintf("cmd-%d", i)))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(idx).To(gomega.Equal(uint64(i + 1)))
			}
			gomega.Expect(waitForReplicationCommitted(captures, proposals+1, 2*time.Second)).To(gomega.Succeed())

			cleanup()
			for _, capture := range captures {
				entries := capture.applied
				gomega.Expect(entries).To(gomega.HaveLen(proposals+1), "%s applied count", capture.node.cfg.ID)
				gomega.Expect(entries[0].Type).To(gomega.Equal(LogEntryNoOp), "%s entries[0] is no-op", capture.node.cfg.ID)
				for i := 1; i <= proposals; i++ {
					entry := entries[i]
					gomega.Expect(entry.Index).To(gomega.Equal(uint64(i+1)), "%s entry[%d].Index", capture.node.cfg.ID, i)
					gomega.Expect(entry.Command).To(gomega.Equal([]byte(fmt.Sprintf("cmd-%d", i))), "%s entry[%d].Command", capture.node.cfg.ID, i)
				}
			}
		}, ginkgo.NodeTimeout(8*time.Second))

		ginkgo.It("commits the leader no-op after election", func() {
			leaderTerm := leader.Term()
			gomega.Expect(waitForReplicationCommitted(captures, 1, 2*time.Second)).To(gomega.Succeed())

			cleanup()
			for _, capture := range captures {
				entries := capture.applied
				gomega.Expect(entries).NotTo(gomega.BeEmpty(), "%s must have at least the no-op", capture.node.cfg.ID)
				noOp := entries[0]
				gomega.Expect(noOp.Index).To(gomega.Equal(uint64(1)), "%s no-op index", capture.node.cfg.ID)
				gomega.Expect(noOp.Term).To(gomega.Equal(leaderTerm), "%s no-op term", capture.node.cfg.ID)
				gomega.Expect(noOp.Type).To(gomega.Equal(LogEntryNoOp), "%s no-op type", capture.node.cfg.ID)
			}
		})

		ginkgo.It("keeps apply order consistent across followers", func(ginkgo.SpecContext) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			const proposals = 3
			for i := 1; i <= proposals; i++ {
				_, err := leader.ProposeWait(ctx, []byte(fmt.Sprintf("v%d", i)))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			gomega.Expect(waitForReplicationCommitted(captures, proposals+1, 2*time.Second)).To(gomega.Succeed())

			cleanup()
			for _, capture := range captures {
				entries := capture.applied
				gomega.Expect(entries).To(gomega.HaveLen(proposals+1), "%s applied count", capture.node.cfg.ID)
				for i, entry := range entries {
					gomega.Expect(entry.Index).To(gomega.Equal(uint64(i+1)), "%s entry[%d].Index", capture.node.cfg.ID, i)
				}
			}
		}, ginkgo.NodeTimeout(8*time.Second))
	})

	ginkgo.Context("transport-driven three-voter cluster", func() {
		ids := []string{"n1", "n2", "n3"}

		defaultConfig := func(i int, id string) Config {
			electionTimeout := slowElectionTimeout
			if i == 0 {
				electionTimeout = fastElectionTimeout
			}
			return Config{
				ID:               id,
				Peers:            otherIDsLocal(ids, id),
				ElectionTimeout:  electionTimeout,
				HeartbeatTimeout: testHeartbeat,
			}
		}

		ginkgo.It("releases pending proposal waiters when the leader steps down", func(ginkgo.SpecContext) {
			nodes, cleanup, err := startReplicationNodes(ids, defaultConfig, func(net *memNetwork, nodes []*Node) {
				for _, node := range nodes {
					transport := net.Register(node.cfg.ID, node)
					if node.cfg.ID == "n1" {
						node.SetTransport(&dropAETransport{inner: transport})
					} else {
						node.SetTransport(transport)
					}
				}
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.DeferCleanup(cleanup)

			leader := nodes[0]
			gomega.Expect(waitFor(2*time.Second, func() bool { return leader.IsLeader() })).To(gomega.Succeed(), "n1 did not become leader")
			leaderTerm := leader.Term()

			resultCh := make(chan error, 1)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			go func() {
				_, err := leader.ProposeWait(ctx, []byte("blocked"))
				resultCh <- err
			}()

			time.Sleep(50 * time.Millisecond)
			reply := leader.HandleAppendEntries(&AppendEntriesArgs{
				Term:     leaderTerm + 5,
				LeaderID: "intruder",
			})
			gomega.Expect(reply.Success).To(gomega.BeTrue(), "AE at higher term must succeed once we step down")
			gomega.Expect(reply.Term).To(gomega.Equal(leaderTerm + 5))
			gomega.Expect(leader.State()).To(gomega.Equal(Follower))

			gomega.Eventually(resultCh, 500*time.Millisecond).Should(gomega.Receive(gomega.MatchError(gomega.MatchRegexp(ErrProposalFailed.Error()))))
		}, ginkgo.NodeTimeout(7*time.Second))

		ginkgo.It("retries replication using conflict hints until followers converge", func(ginkgo.SpecContext) {
			countingTransport := &countingAETransport{count: make(map[string]int)}

			nodes, cleanup, err := startReplicationNodes(ids, defaultConfig, func(net *memNetwork, nodes []*Node) {
				seedLogEntries(nodes[0], []LogEntry{{Term: 5, Index: 1, Command: []byte("leader-old")}})
				nodes[0].st.currentTerm = 5
				nodes[0].rs.Store(nodes[0].st.snapshot())

				seedLogEntries(nodes[1], []LogEntry{{Term: 3, Index: 1, Command: []byte("stale")}})
				nodes[1].st.currentTerm = 3
				nodes[1].rs.Store(nodes[1].st.snapshot())

				for _, node := range nodes {
					transport := net.Register(node.cfg.ID, node)
					if node.cfg.ID == "n1" {
						countingTransport.inner = transport
						node.SetTransport(countingTransport)
					} else {
						node.SetTransport(transport)
					}
				}
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.DeferCleanup(cleanup)

			leader := nodes[0]
			gomega.Expect(waitFor(2*time.Second, func() bool { return leader.IsLeader() })).To(gomega.Succeed(), "n1 did not become leader")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			idx, err := leader.ProposeWait(ctx, []byte("newcmd"))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(idx).To(gomega.Equal(uint64(3)), "seeded entry + no-op + user entry")
			gomega.Expect(waitFor(2*time.Second, func() bool {
				for _, node := range nodes {
					if node.CommittedIndex() < 3 {
						return false
					}
				}
				return true
			})).To(gomega.Succeed(), "not all nodes reached commitIndex 3")

			nodes[1].Stop()
			entry := mustLogEntry(nodes[1], 1)
			gomega.Expect(nodes[1].st.log.LastIndex()).To(gomega.BeNumerically(">=", uint64(3)), "n2 should have at least 3 entries")
			gomega.Expect(entry.Term).To(gomega.Equal(uint64(5)), "n2's stale index-1 term should be replaced")
			gomega.Expect(entry.Command).To(gomega.Equal([]byte("leader-old")), "n2's index-1 command should match n1's seed")
			countingTransport.mu.Lock()
			n2Traffic := countingTransport.count["n2"]
			countingTransport.mu.Unlock()
			gomega.Expect(n2Traffic).To(gomega.BeNumerically(">", 0), "AE wrapper should have observed traffic to n2")
		}, ginkgo.NodeTimeout(8*time.Second))

		ginkgo.It("caps AppendEntries batches at MaxEntriesPerAE", func(ginkgo.SpecContext) {
			const maxEntries = 64
			capture := &capturingAETransport{maxSeen: make(map[string]int)}
			nodes, cleanup, err := startReplicationNodes(ids, func(i int, id string) Config {
				cfg := defaultConfig(i, id)
				cfg.MaxEntriesPerAE = maxEntries
				return cfg
			}, func(net *memNetwork, nodes []*Node) {
				capture.inner = net.Register("n1", nodes[0])
				nodes[0].SetTransport(capture)
				for _, node := range nodes[1:] {
					node.SetTransport(net.Register(node.cfg.ID, node))
				}
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.DeferCleanup(cleanup)

			leader := nodes[0]
			gomega.Expect(waitFor(2*time.Second, func() bool { return leader.IsLeader() })).To(gomega.Succeed())
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			const proposals = 200
			for i := 0; i < proposals; i++ {
				_, err := leader.ProposeWait(ctx, []byte(fmt.Sprintf("entry-%d", i)))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			gomega.Expect(waitFor(5*time.Second, func() bool {
				for _, node := range nodes {
					if node.CommittedIndex() < proposals {
						return false
					}
				}
				return true
			})).To(gomega.Succeed(), "not all nodes committed proposals")

			capture.mu.Lock()
			maxN2 := capture.maxSeen["n2"]
			maxN3 := capture.maxSeen["n3"]
			capture.mu.Unlock()
			gomega.Expect(maxN2).To(gomega.BeNumerically("<=", maxEntries), "n2 max batch size")
			gomega.Expect(maxN3).To(gomega.BeNumerically("<=", maxEntries), "n3 max batch size")
		}, ginkgo.NodeTimeout(15*time.Second))

		ginkgo.It("dispatches a pending entry immediately after heartbeat replies clear in-flight state", func(ginkgo.SpecContext) {
			releaseCh := make(chan struct{})
			blocker := &blockEmptyAETransport{
				blockPeers: map[string]bool{"n2": true, "n3": true},
				releaseCh:  releaseCh,
				blockedCh:  make(chan string, 8),
			}
			released := false
			ginkgo.DeferCleanup(func() {
				if !released {
					close(releaseCh)
					released = true
				}
			})

			nodes, cleanup, err := startReplicationNodes(ids, func(i int, id string) Config {
				cfg := defaultConfig(i, id)
				cfg.ElectionTimeout = 2 * time.Second
				if i == 0 {
					cfg.ElectionTimeout = fastElectionTimeout
				}
				cfg.HeartbeatTimeout = 500 * time.Millisecond
				return cfg
			}, func(net *memNetwork, nodes []*Node) {
				blocker.inner = net.Register("n1", nodes[0])
				nodes[0].SetTransport(blocker)
				nodes[1].SetTransport(net.Register("n2", nodes[1]))
				nodes[2].SetTransport(net.Register("n3", nodes[2]))
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.DeferCleanup(cleanup)

			leader := nodes[0]
			gomega.Expect(waitFor(2*time.Second, func() bool { return leader.IsLeader() })).To(gomega.Succeed(), "n1 did not become leader")
			gomega.Expect(waitFor(2*time.Second, func() bool { return leader.CommittedIndex() >= 1 })).To(gomega.Succeed(), "leader no-op did not commit")

			blocker.armed.Store(true)
			seen := map[string]bool{}
			gomega.Expect(waitFor(2*time.Second, func() bool {
				for {
					select {
					case peer := <-blocker.blockedCh:
						seen[peer] = true
					default:
						return seen["n2"] && seen["n3"]
					}
				}
			})).To(gomega.Succeed(), "expected empty heartbeats to both followers to be blocked")

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			doneCh := make(chan error, 1)
			go func() {
				_, err := leader.ProposeWait(ctx, []byte("after-heartbeat"))
				doneCh <- err
			}()

			gomega.Consistently(doneCh, 50*time.Millisecond).ShouldNot(gomega.Receive(), "proposal completed before blocked heartbeats were released")
			close(releaseCh)
			released = true
			gomega.Eventually(doneCh, 120*time.Millisecond).Should(gomega.Receive(gomega.Succeed()), "proposal waited for a later heartbeat tick")
		}, ginkgo.NodeTimeout(6*time.Second))

		ginkgo.It("dispatches LeaderCommit after a follower entry reply returns", func(ginkgo.SpecContext) {
			releaseCh := make(chan struct{})
			blocker := &blockEntryReplyTransport{
				blockPeer: "n3",
				releaseCh: releaseCh,
				blockedCh: make(chan struct{}, 1),
			}
			released := false
			ginkgo.DeferCleanup(func() {
				if !released {
					close(releaseCh)
					released = true
				}
			})

			nodes, cleanup, err := startReplicationNodes(ids, func(i int, id string) Config {
				cfg := defaultConfig(i, id)
				cfg.ElectionTimeout = 2 * time.Second
				if i == 0 {
					cfg.ElectionTimeout = fastElectionTimeout
				}
				cfg.HeartbeatTimeout = 500 * time.Millisecond
				return cfg
			}, func(net *memNetwork, nodes []*Node) {
				blocker.inner = net.Register("n1", nodes[0])
				nodes[0].SetTransport(blocker)
				nodes[1].SetTransport(net.Register("n2", nodes[1]))
				nodes[2].SetTransport(net.Register("n3", nodes[2]))
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.DeferCleanup(cleanup)

			leader, node2, node3 := nodes[0], nodes[1], nodes[2]
			gomega.Expect(waitFor(2*time.Second, func() bool { return leader.IsLeader() })).To(gomega.Succeed(), "n1 did not become leader")
			gomega.Expect(waitFor(2*time.Second, func() bool {
				return leader.CommittedIndex() >= 1 && node2.CommittedIndex() >= 1 && node3.CommittedIndex() >= 1
			})).To(gomega.Succeed(), "leader no-op did not apply across cluster")

			blocker.armed.Store(true)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, err = leader.ProposeWait(ctx, []byte("commit-notify"))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			gomega.Eventually(blocker.blockedCh, 2*time.Second).Should(gomega.Receive(), "expected n3 entry reply to be blocked")
			gomega.Expect(waitFor(2*time.Second, func() bool {
				return leader.CommittedIndex() >= 2 && node2.CommittedIndex() >= 2
			})).To(gomega.Succeed(), "proposal did not commit via n2")
			gomega.Expect(node3.CommittedIndex()).To(gomega.BeNumerically("<", uint64(2)), "n3 should have appended but not applied before commit notification")

			close(releaseCh)
			released = true
			gomega.Expect(waitFor(120*time.Millisecond, func() bool {
				return node3.CommittedIndex() >= 2
			})).To(gomega.Succeed(), "n3 waited for a later heartbeat tick to learn LeaderCommit")
		}, ginkgo.NodeTimeout(6*time.Second))

		ginkgo.It("keeps at most one AppendEntries goroutine per partitioned peer", func(ginkgo.SpecContext) {
			releaseCh := make(chan struct{})
			blocker := &blockingAETransport{
				blockPeer: "n2",
				releaseCh: releaseCh,
			}
			released := false
			ginkgo.DeferCleanup(func() {
				if !released {
					close(releaseCh)
					released = true
				}
			})

			nodes, cleanup, err := startReplicationNodes(ids, defaultConfig, func(net *memNetwork, nodes []*Node) {
				blocker.inner = net.Register("n1", nodes[0])
				nodes[0].SetTransport(blocker)
				nodes[1].SetTransport(net.Register("n2", nodes[1]))
				nodes[2].SetTransport(net.Register("n3", nodes[2]))
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.DeferCleanup(cleanup)

			leader := nodes[0]
			gomega.Expect(waitFor(2*time.Second, func() bool { return leader.IsLeader() })).To(gomega.Succeed(), "n1 did not become leader")
			time.Sleep(20 * testHeartbeat)

			close(releaseCh)
			released = true
			gomega.Expect(waitFor(2*time.Second, func() bool {
				return blocker.inFlight.Load() == 0
			})).To(gomega.Succeed(), "blocked goroutines did not drain after release")
			gomega.Expect(blocker.maxInFlight.Load()).To(gomega.BeNumerically("<=", int64(1)), "per-peer single-flight gate must bound concurrent AE goroutines")
		}, ginkgo.NodeTimeout(6*time.Second))
	})
})

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
