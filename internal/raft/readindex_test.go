package raft

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("ReadIndex", func() {
	ginkgo.It("rejects follower reads without queueing actor work", func(ginkgo.SpecContext) {
		nodes, _, cleanup, err := startRaftIntegrationCluster("n1", "n2", "n3")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(cleanup)

		n1, n2 := nodes[0], nodes[1]
		gomega.Expect(waitFor(2*time.Second, n1.IsLeader)).To(gomega.Succeed())
		gomega.Expect(n2.IsLeader()).To(gomega.BeFalse())

		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		idx, err := n2.ReadIndex(ctx)
		gomega.Expect(err).To(gomega.MatchError(gomega.MatchRegexp(ErrNotLeader.Error())))
		gomega.Expect(errors.Is(err, ErrNotLeader)).To(gomega.BeTrue())
		gomega.Expect(idx).To(gomega.Equal(uint64(0)))
	}, ginkgo.NodeTimeout(5*time.Second))

	ginkgo.It("returns the commit index inline for a single-voter leader", func(ginkgo.SpecContext) {
		node, cleanup, err := startRaftIntegrationSingleVoter("solo")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(cleanup)

		pctx, pcancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer pcancel()
		idx, err := node.ProposeWait(pctx, []byte("v1"))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(idx).To(gomega.Equal(uint64(1)), "single-voter skips becomeLeader no-op")

		rctx, rcancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer rcancel()
		got, err := node.ReadIndex(rctx)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(got).To(gomega.BeNumerically(">=", 1), "barrier must reflect at least the proposed entry")
	}, ginkgo.NodeTimeout(5*time.Second))

	ginkgo.It("confirms a multi-voter leader through a heartbeat round", func(ginkgo.SpecContext) {
		nodes, _, cleanup, err := startRaftIntegrationCluster("n1", "n2", "n3")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(cleanup)

		leader := nodes[0]
		gomega.Expect(waitFor(2*time.Second, leader.IsLeader)).To(gomega.Succeed())

		pctx, pcancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer pcancel()
		_, err = leader.ProposeWait(pctx, []byte("commit-1"))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		commitBefore := leader.CommittedIndex()
		gomega.Expect(commitBefore).To(gomega.BeNumerically(">=", 1))

		rctx, rcancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer rcancel()
		idx, err := leader.ReadIndex(rctx)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(idx).To(gomega.BeNumerically(">=", commitBefore),
			"ReadIndex must return at least the commit index that existed at request time")
	}, ginkgo.NodeTimeout(5*time.Second))

	ginkgo.It("drains queued reads when a partitioned leader steps down", func(ginkgo.SpecContext) {
		net := newMemNetwork()
		ids := []string{"n1", "n2", "n3"}
		nodes := make([]*Node, 3)
		for i, id := range ids {
			peers := otherIDsLocal(ids, id)
			electionTimeout := slowElectionTimeout
			if id == "n1" {
				electionTimeout = fastElectionTimeout
			}
			node, err := NewNode(Config{
				ID:               id,
				Peers:            peers,
				ElectionTimeout:  electionTimeout,
				HeartbeatTimeout: testHeartbeat,
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			nodes[i] = node
		}

		var wg sync.WaitGroup
		for _, node := range nodes {
			tr := net.Register(node.cfg.ID, node)
			if node.cfg.ID == "n1" {
				node.SetTransport(&dropAETransport{inner: tr})
			} else {
				node.SetTransport(tr)
			}
		}
		for _, node := range nodes {
			node.Start()
			wg.Add(1)
			go func(node *Node) {
				defer wg.Done()
				for range node.ApplyCh() {
				}
			}(node)
		}
		defer func() {
			for i := len(nodes) - 1; i >= 0; i-- {
				nodes[i].Stop()
			}
			wg.Wait()
		}()

		n1 := nodes[0]
		gomega.Expect(waitFor(2*time.Second, n1.IsLeader)).To(gomega.Succeed())
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
		gomega.Expect(reply.Success).To(gomega.BeTrue())
		gomega.Expect(n1.State()).To(gomega.Equal(Follower))

		select {
		case result := <-resCh:
			gomega.Expect(result.err).To(gomega.HaveOccurred())
			gomega.Expect(errors.Is(result.err, ErrProposalFailed) || errors.Is(result.err, ErrNotLeader)).
				To(gomega.BeTrue(), "ReadIndex after step-down must surface a leader-loss error, got: %v", result.err)
		case <-time.After(500 * time.Millisecond):
			ginkgo.Fail("ReadIndex hung after Leader step-down; queue not drained")
		}
	}, ginkgo.NodeTimeout(10*time.Second))

	ginkgo.It("ignores stale heartbeat round replies", func(ginkgo.SpecContext) {
		node, err := NewNode(Config{
			ID:               "n1",
			Peers:            []string{"p1", "p2"},
			ElectionTimeout:  time.Hour,
			HeartbeatTimeout: testHeartbeat,
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		node.st.currentTerm = 5
		node.st.state = Leader
		node.st.leaderID = "n1"
		node.st.matchIndex = map[string]uint64{"p1": 0, "p2": 0}
		node.st.nextIndex = map[string]uint64{"p1": 1, "p2": 1}
		node.st.peerInFlight = map[string]bool{"p1": false, "p2": false}
		node.st.peerLastRound = map[string]uint64{"p1": 0, "p2": 0}
		node.st.leaderRound = 0
		node.rs.Store(node.st.snapshot())

		node.handleHeartbeatReply(command{
			kind:           cmdHeartbeatReply,
			hbPeer:         "p1",
			hbTerm:         4,
			hbDispatchTerm: 4,
			hbRoundID:      999,
			hbSuccess:      true,
			hbMatchAfter:   0,
		})
		gomega.Expect(node.st.peerLastRound["p1"]).To(gomega.Equal(uint64(0)))
		gomega.Expect(node.st.peerLastRound["p2"]).To(gomega.Equal(uint64(0)))

		node.handleHeartbeatReply(command{
			kind:           cmdHeartbeatReply,
			hbPeer:         "p1",
			hbTerm:         5,
			hbDispatchTerm: 5,
			hbRoundID:      7,
			hbSuccess:      true,
			hbMatchAfter:   0,
		})
		gomega.Expect(node.st.peerLastRound["p1"]).To(gomega.Equal(uint64(7)))
	})

	ginkgo.It("resets peerLastRound when leadership is regained", func(ginkgo.SpecContext) {
		nodes, _, cleanup, err := startRaftIntegrationCluster("x1", "x2", "x3")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(cleanup)

		leader := nodes[0]
		gomega.Expect(waitFor(2*time.Second, leader.IsLeader)).To(gomega.Succeed())
		leaderTerm := leader.Term()

		pctx, pcancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer pcancel()
		_, err = leader.ReadIndex(pctx)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		reply := leader.HandleAppendEntries(&AppendEntriesArgs{
			Term:     leaderTerm + 10,
			LeaderID: "intruder",
		})
		gomega.Expect(reply.Success).To(gomega.BeTrue())
		gomega.Expect(leader.State()).To(gomega.Equal(Follower))

		gomega.Expect(waitFor(5*time.Second, func() bool {
			for _, node := range nodes {
				if node.IsLeader() && node.Term() > leaderTerm {
					return true
				}
			}
			return false
		})).To(gomega.Succeed(), "no new leader emerged at higher term")

		var newLeader *Node
		for _, node := range nodes {
			if node.IsLeader() {
				newLeader = node
				break
			}
		}
		gomega.Expect(newLeader).NotTo(gomega.BeNil())

		rctx, rcancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer rcancel()
		idx, err := newLeader.ReadIndex(rctx)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(idx).To(gomega.BeNumerically(">=", 0))
	}, ginkgo.NodeTimeout(10*time.Second))

	ginkgo.Context("apply pipeline", func() {
		ginkgo.It("keeps actor commands live while a slow FSM consumer drains entries", func(ginkgo.SpecContext) {
			net := newMemNetwork()
			ids := []string{"a1", "a2", "a3"}
			nodes := make([]*Node, 3)
			for i, id := range ids {
				peers := otherIDsLocal(ids, id)
				electionTimeout := slowElectionTimeout
				if i == 0 {
					electionTimeout = fastElectionTimeout
				}
				node, err := NewNode(Config{
					ID:               id,
					Peers:            peers,
					ElectionTimeout:  electionTimeout,
					HeartbeatTimeout: testHeartbeat,
				})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				nodes[i] = node
			}
			for _, node := range nodes {
				node.SetTransport(net.Register(node.cfg.ID, node))
			}
			for _, node := range nodes {
				node.Start()
			}
			defer func() {
				for i := len(nodes) - 1; i >= 0; i-- {
					nodes[i].Stop()
				}
			}()

			leader := nodes[0]
			go func() {
				for entry := range leader.ApplyCh() {
					_ = entry
					time.Sleep(5 * time.Millisecond)
				}
			}()
			for _, node := range nodes[1:] {
				go func(node *Node) {
					for range node.ApplyCh() {
					}
				}(node)
			}

			gomega.Expect(waitFor(2*time.Second, leader.IsLeader)).To(gomega.Succeed())

			const n = 200
			pctx, pcancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer pcancel()
			for i := 0; i < n; i++ {
				_, err := leader.ProposeWait(pctx, []byte{byte(i)})
				gomega.Expect(err).NotTo(gomega.HaveOccurred(), "propose %d failed; actor likely wedged on slow FSM", i)
			}

			rctx, rcancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer rcancel()
			idx, err := leader.ReadIndex(rctx)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "ReadIndex during slow apply drain; actor wedged?")
			gomega.Expect(idx).To(gomega.BeNumerically(">=", n), "barrier must reflect all committed entries")
		}, ginkgo.NodeTimeout(10*time.Second))

		ginkgo.It("preserves FIFO delivery with a slow FSM consumer", func(ginkgo.SpecContext) {
			node, err := NewNode(Config{ID: "n1"})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			node.Start()
			ginkgo.DeferCleanup(node.Stop)

			gomega.Expect(waitFor(time.Second, node.IsLeader)).To(gomega.Succeed())

			const n = 200
			delivered := make([]uint64, 0, n)
			deliveredMu := sync.Mutex{}
			done := make(chan struct{})
			go func() {
				count := 0
				for entry := range node.ApplyCh() {
					if entry.Type == LogEntryNoOp {
						continue
					}
					time.Sleep(time.Millisecond)
					deliveredMu.Lock()
					delivered = append(delivered, entry.Index)
					count++
					current := count
					deliveredMu.Unlock()
					if current >= n {
						close(done)
						return
					}
				}
			}()

			pctx, pcancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer pcancel()
			for i := 0; i < n; i++ {
				_, err := node.ProposeWait(pctx, []byte{byte(i)})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			select {
			case <-done:
			case <-time.After(5 * time.Second):
				ginkgo.Fail(fmt.Sprintf("apply consumer didn't see all %d entries", n))
			}

			deliveredMu.Lock()
			defer deliveredMu.Unlock()
			gomega.Expect(delivered).To(gomega.HaveLen(n))
			for i := 1; i < len(delivered); i++ {
				gomega.Expect(delivered[i]).To(gomega.BeNumerically(">", delivered[i-1]),
					"applyCh delivery must be FIFO by Index; got delivered[%d]=%d, delivered[%d]=%d",
					i-1, delivered[i-1], i, delivered[i])
			}
		}, ginkgo.NodeTimeout(15*time.Second))

		ginkgo.It("flushes ready entries during apply loop shutdown", func() {
			for attempt := 0; attempt < 50; attempt++ {
				node, err := NewNode(Config{ID: fmt.Sprintf("n%d", attempt)})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				go node.applyLoop()

				want := []LogEntry{
					{Term: 1, Index: 1, Type: LogEntryNoOp},
					{Term: 1, Index: 2, Command: []byte("v1")},
					{Term: 1, Index: 3, Command: []byte("v2")},
					{Term: 1, Index: 4, Command: []byte("v3")},
				}
				for _, entry := range want {
					node.applyInCh <- entry
				}

				close(node.stopCh)
				close(node.applyInCh)

				var got []LogEntry
				for entry := range node.ApplyCh() {
					got = append(got, entry)
				}
				gomega.Expect(got).To(gomega.Equal(want))
			}
		})
	})
})
