package raft

import (
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// openTestDB opens a Badger DB at dir with no logger. It remains as a
// testing.TB helper for older non-Ginkgo tests that still share it.
func openTestDB(t testing.TB, dir string) (*badger.DB, func()) {
	t.Helper()
	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	if err != nil {
		t.Fatalf("open badger: %v", err)
	}
	return db, func() { _ = db.Close() }
}

func openRecoveryDB(dir string) (*badger.DB, func(), error) {
	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	if err != nil {
		return nil, nil, err
	}
	return db, func() { _ = db.Close() }, nil
}

func newRecoveryPersistentNode(dir, id string, peers []string, electionTimeout time.Duration) (*Node, func(), error) {
	db, closeDB, err := openRecoveryDB(dir)
	if err != nil {
		return nil, nil, err
	}

	logStore, err := newBadgerLogStore(db, []byte("raft/v2/log/"))
	if err != nil {
		closeDB()
		return nil, nil, err
	}
	stable, err := newBadgerStableStore(db, []byte("raft/v2/hardstate/"))
	if err != nil {
		closeDB()
		return nil, nil, err
	}

	node, err := NewNode(Config{
		ID:               id,
		Peers:            peers,
		LogStore:         logStore,
		StableStore:      stable,
		ElectionTimeout:  electionTimeout,
		HeartbeatTimeout: testHeartbeat,
	})
	if err != nil {
		closeDB()
		return nil, nil, err
	}

	cleanup := func() {
		node.Stop()
		closeDB()
	}
	return node, cleanup, nil
}

var _ = ginkgo.Describe("Recovery", func() {
	var dir string

	ginkgo.BeforeEach(func() {
		var err error
		dir, err = os.MkdirTemp("", "raft-recovery-*")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(os.RemoveAll, dir)
	})

	ginkgo.It("starts fresh with empty badger stores", func(ginkgo.SpecContext) {
		node, cleanup, err := newRecoveryPersistentNode(dir, "n1", nil, 0)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(cleanup)
		node.Start()

		gomega.Expect(waitFor(time.Second, node.IsLeader)).To(gomega.Succeed())
		gomega.Expect(node.Term()).To(gomega.Equal(uint64(1)))
		gomega.Expect(node.State()).To(gomega.Equal(Leader))
	}, ginkgo.NodeTimeout(5*time.Second))

	ginkgo.It("replays log and hard state from badger stores", func(ginkgo.SpecContext) {
		var afterFirstRun uint64

		func() {
			db, closeDB, err := openRecoveryDB(dir)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			logStore, err := newBadgerLogStore(db, []byte("raft/v2/log/"))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			stable, err := newBadgerStableStore(db, []byte("raft/v2/hardstate/"))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			node, err := NewNode(Config{
				ID:          "n1",
				LogStore:    logStore,
				StableStore: stable,
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			node.Start()
			go func(node *Node) {
				for range node.ApplyCh() {
				}
			}(node)

			gomega.Expect(waitFor(time.Second, node.IsLeader)).To(gomega.Succeed())
			for i := 0; i < 3; i++ {
				gomega.Expect(node.Propose([]byte("cmd"))).To(gomega.Succeed())
			}
			gomega.Expect(waitFor(2*time.Second, func() bool {
				return node.CommittedIndex() >= 3
			})).To(gomega.Succeed())

			afterFirstRun = node.st.log.LastIndex()
			gomega.Expect(afterFirstRun).To(gomega.Equal(uint64(3)))
			node.Stop()
			closeDB()
		}()

		db, closeDB, err := openRecoveryDB(dir)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(closeDB)

		logStore, err := newBadgerLogStore(db, []byte("raft/v2/log/"))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		stable, err := newBadgerStableStore(db, []byte("raft/v2/hardstate/"))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		node, err := NewNode(Config{
			ID:          "n1",
			LogStore:    logStore,
			StableStore: stable,
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(node.st.log.LastIndex()).To(gomega.Equal(uint64(3)))
		gomega.Expect(node.st.currentTerm).To(gomega.Equal(uint64(1)))

		node.Start()
		ginkgo.DeferCleanup(node.Stop)
		go func(node *Node) {
			for range node.ApplyCh() {
			}
		}(node)

		gomega.Expect(waitFor(time.Second, node.IsLeader)).To(gomega.Succeed())
		gomega.Expect(node.Term()).To(gomega.Equal(uint64(1)), "term must remain 1 after restart")
		_ = afterFirstRun
	}, ginkgo.NodeTimeout(10*time.Second))

	ginkgo.It("persists HardState across an election vote", func(ginkgo.SpecContext) {
		net := newMemNetwork()
		n2dir, err := os.MkdirTemp("", "raft-recovery-n2-*")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(os.RemoveAll, n2dir)

		n1, err := NewNode(Config{
			ID:               "n1",
			Peers:            []string{"n2", "n3"},
			ElectionTimeout:  fastElectionTimeout,
			HeartbeatTimeout: testHeartbeat,
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		n2db, closeN2db, err := openRecoveryDB(n2dir)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		n2stable, err := newBadgerStableStore(n2db, []byte("raft/v2/hardstate/"))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		n2log, err := newBadgerLogStore(n2db, []byte("raft/v2/log/"))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		n2, err := NewNode(Config{
			ID:               "n2",
			Peers:            []string{"n1", "n3"},
			ElectionTimeout:  slowElectionTimeout,
			HeartbeatTimeout: testHeartbeat,
			LogStore:         n2log,
			StableStore:      n2stable,
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		n3, err := NewNode(Config{
			ID:               "n3",
			Peers:            []string{"n1", "n2"},
			ElectionTimeout:  slowElectionTimeout,
			HeartbeatTimeout: testHeartbeat,
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		for _, node := range []*Node{n1, n2, n3} {
			node.SetTransport(net.Register(node.cfg.ID, node))
		}
		for _, node := range []*Node{n1, n2, n3} {
			node.Start()
			go func(node *Node) {
				for range node.ApplyCh() {
				}
			}(node)
		}

		gomega.Expect(waitFor(2*time.Second, n1.IsLeader)).To(gomega.Succeed(), "n1 did not win election")
		electedTerm := n1.Term()
		gomega.Expect(waitFor(time.Second, func() bool {
			return n2.rs.Load().term == electedTerm
		})).To(gomega.Succeed(), "n2 must catch up to elected term")

		hs2, err := n2stable.HardState()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(hs2.CurrentTerm).To(gomega.Equal(electedTerm), "n2 stable store must be at elected term")
		gomega.Expect(hs2.VotedFor).To(gomega.Equal("n1"), "n2 must have voted for n1 in stable store")

		n2.Stop()
		closeN2db()
		n1.Stop()
		n3.Stop()

		n2db2, closeN2db2, err := openRecoveryDB(n2dir)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(closeN2db2)
		n2stable2, err := newBadgerStableStore(n2db2, []byte("raft/v2/hardstate/"))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		n2log2, err := newBadgerLogStore(n2db2, []byte("raft/v2/log/"))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		hs, err := n2stable2.HardState()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(hs.CurrentTerm).To(gomega.Equal(electedTerm))
		gomega.Expect(hs.VotedFor).To(gomega.Equal("n1"))

		recovered, err := NewNode(Config{
			ID:               "n2",
			Peers:            []string{"n1", "n3"},
			ElectionTimeout:  slowElectionTimeout,
			HeartbeatTimeout: testHeartbeat,
			LogStore:         n2log2,
			StableStore:      n2stable2,
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		recovered.Start()
		ginkgo.DeferCleanup(recovered.Stop)
		go func(node *Node) {
			for range node.ApplyCh() {
			}
		}(recovered)

		done := make(chan *RequestVoteReply, 1)
		go func() {
			done <- recovered.HandleRequestVote(&RequestVoteArgs{
				Term:         electedTerm,
				CandidateID:  "z-candidate",
				LastLogIndex: 1000,
				LastLogTerm:  1000,
			})
		}()
		select {
		case reply := <-done:
			gomega.Expect(reply.VoteGranted).To(gomega.BeFalse(),
				"restarted n2 must deny vote to z-candidate at same term T")
		case <-time.After(2 * time.Second):
			ginkgo.Fail("HandleRequestVote timed out")
		}
	}, ginkgo.NodeTimeout(15*time.Second))

	ginkgo.It("recovers an orphaned PromoteToVoter after leader crash", func(ginkgo.SpecContext) {
		net := newMemNetwork()
		var nodes []*Node
		var mu sync.Mutex

		makeNode := func(id string, peers []string, electionTimeout time.Duration) *Node {
			node, err := NewNode(Config{
				ID:               id,
				Peers:            peers,
				ElectionTimeout:  electionTimeout,
				HeartbeatTimeout: testHeartbeat,
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			node.SetTransport(net.Register(id, node))
			node.Start()
			mu.Lock()
			nodes = append(nodes, node)
			mu.Unlock()
			go func(node *Node) {
				for range node.ApplyCh() {
				}
			}(node)
			return node
		}
		ginkgo.DeferCleanup(func() {
			for i := len(nodes) - 1; i >= 0; i-- {
				nodes[i].Stop()
			}
		})

		n1 := makeNode("n1", []string{"n2", "n3"}, fastElectionTimeout)
		n2 := makeNode("n2", []string{"n1", "n3"}, slowElectionTimeout)
		n3 := makeNode("n3", []string{"n1", "n2"}, slowElectionTimeout)

		gomega.Expect(waitFor(2*time.Second, n1.IsLeader)).To(gomega.Succeed(), "n1 must become leader")

		n4 := makeNode("n4", []string{"n1"}, slowElectionTimeout)
		gomega.Expect(n1.AddLearner("n4", "n4-addr")).To(gomega.Succeed())
		gomega.Expect(waitFor(3*time.Second, func() bool {
			return n1.peerMatchIndexForTest("n4") >= n1.CommittedIndex()
		})).To(gomega.Succeed(), "n4 must catch up before promote")
		catchupCommit := n1.CommittedIndex()

		drop1 := newDropJointTransport(n1.loadTransport())
		drop1.enable()
		n1.SetTransport(drop1)

		promoteDone := make(chan error, 1)
		go func() { promoteDone <- n1.PromoteToVoter("n4") }()

		gomega.Expect(waitFor(3*time.Second, func() bool {
			return n1.CommittedIndex() > catchupCommit
		})).To(gomega.Succeed(), "Stage-1 must commit on n1")

		n1.Stop()
		select {
		case <-promoteDone:
		case <-time.After(2 * time.Second):
		}
		drop1.disable()

		newLeader := func() *Node {
			for _, node := range []*Node{n2, n3} {
				if node.IsLeader() {
					return node
				}
			}
			return nil
		}
		gomega.Expect(waitFor(5*time.Second, func() bool { return newLeader() != nil })).
			To(gomega.Succeed(), "n2 or n3 must elect a new leader")

		gomega.Expect(waitFor(5*time.Second, func() bool {
			for _, node := range []*Node{n2, n3, n4} {
				found := false
				for _, server := range node.Configuration().Servers {
					if server.ID == "n4" && server.Suffrage == Voter {
						found = true
						break
					}
				}
				if !found {
					return false
				}
			}
			return true
		})).To(gomega.Succeed(), "n4 must appear as Voter after orphan recovery")
	}, ginkgo.NodeTimeout(15*time.Second))

	ginkgo.It("restarts a single-voter leader as follower before auto-promotion", func(ginkgo.SpecContext) {
		func() {
			db, closeDB, err := openRecoveryDB(dir)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			stable, err := newBadgerStableStore(db, []byte("raft/v2/hardstate/"))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			node, err := NewNode(Config{ID: "n1", StableStore: stable})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			node.Start()
			go func(node *Node) {
				for range node.ApplyCh() {
				}
			}(node)
			gomega.Expect(waitFor(time.Second, node.IsLeader)).To(gomega.Succeed())
			node.Stop()
			closeDB()
		}()

		db, closeDB, err := openRecoveryDB(dir)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(closeDB)
		stable, err := newBadgerStableStore(db, []byte("raft/v2/hardstate/"))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		node, err := NewNode(Config{ID: "n1", StableStore: stable})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		gomega.Expect(node.State()).To(gomega.Equal(Follower), "Node must start as Follower before actor runs")
		node.Start()
		ginkgo.DeferCleanup(node.Stop)
		go func(node *Node) {
			for range node.ApplyCh() {
			}
		}(node)

		gomega.Expect(waitFor(time.Second, node.IsLeader)).To(gomega.Succeed(),
			"restarted single-voter must auto-promote to Leader")
		gomega.Expect(node.Term()).To(gomega.Equal(uint64(1)),
			"term must remain 1 (persisted, not advanced on restart)")
	}, ginkgo.NodeTimeout(10*time.Second))
})

// dropJointTransport is a test-only Transport wrapper that drops any
// AppendEntries RPC that carries a LogEntryJointConfChange entry when armed.
type dropJointTransport struct {
	inner   Transport
	enabled atomic.Bool
}

func newDropJointTransport(inner Transport) *dropJointTransport {
	return &dropJointTransport{inner: inner}
}

func (d *dropJointTransport) enable()  { d.enabled.Store(true) }
func (d *dropJointTransport) disable() { d.enabled.Store(false) }

func (d *dropJointTransport) SendAppendEntries(peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	if d.enabled.Load() {
		for _, entry := range args.Entries {
			if entry.Type == LogEntryJointConfChange {
				return nil, errors.New("joint entry dropped by test")
			}
		}
	}
	return d.inner.SendAppendEntries(peer, args)
}

func (d *dropJointTransport) SendRequestVote(peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	return d.inner.SendRequestVote(peer, args)
}

func (d *dropJointTransport) SendInstallSnapshot(peer string, args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	return d.inner.SendInstallSnapshot(peer, args)
}

func (d *dropJointTransport) SendTimeoutNow(peer string, args *TimeoutNowArgs) (*TimeoutNowReply, error) {
	return d.inner.SendTimeoutNow(peer, args)
}
