package raft

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/gritive/GrainFS/internal/badgerutil"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Snapshot scenarios", func() {
	ginkgo.It("creates a snapshot and compacts a single-voter log", func(ginkgo.SpecContext) {
		node, cleanup, err := startRaftIntegrationSingleVoter("n1")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		cleaned := false
		ginkgo.DeferCleanup(func() {
			if !cleaned {
				cleanup()
			}
		})

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for i := 1; i <= 10; i++ {
			_, err := node.ProposeWait(ctx, []byte(fmt.Sprintf("cmd-%d", i)))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		gomega.Expect(waitFor(2*time.Second, func() bool {
			return node.CommittedIndex() >= 10
		})).To(gomega.Succeed())

		gomega.Expect(node.CreateSnapshot(5, []byte("fsm-state-at-5"))).To(gomega.Succeed())
		snap, err := node.LatestSnapshot()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(snap).NotTo(gomega.BeNil())
		gomega.Expect(snap.LastIncludedIndex).To(gomega.Equal(uint64(5)))
		gomega.Expect(snap.Data).To(gomega.Equal([]byte("fsm-state-at-5")))
		gomega.Expect(snap.Configuration).To(gomega.Equal([]string{"n1"}))

		cleanup()
		cleaned = true

		gomega.Expect(node.st.log.FirstIndex()).To(gomega.Equal(uint64(6)))
		for i := uint64(1); i <= 5; i++ {
			_, err := node.st.log.Entry(i)
			gomega.Expect(err).To(gomega.MatchError(ErrLogIndexOutOfRange), "Entry(%d) must be compacted", i)
		}
		for i := uint64(6); i <= 10; i++ {
			entry, err := node.st.log.Entry(i)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Entry(%d) must still be readable", i)
			gomega.Expect(entry.Index).To(gomega.Equal(i))
		}
		term, err := node.st.log.TermAt(5)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(term).To(gomega.Equal(snap.LastIncludedTerm), "TermAt(boundary) must equal snapshot term")
	}, ginkgo.NodeTimeout(10*time.Second))

	ginkgo.It("preserves learner metadata when creating a snapshot", func() {
		node, err := NewNode(Config{ID: "n1"})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(node.st.log.Append([]LogEntry{{Term: 1, Index: 1, Command: []byte("x")}})).To(gomega.Succeed())
		node.st.commitIndex = 1
		node.st.currentConfig = effectiveConfig{
			voters:   []string{"n1"},
			learners: map[string]string{"n2": "addr2"},
		}

		reply := make(chan error, 1)
		node.handleCreateSnapshot(command{kind: cmdCreateSnapshot, csIndex: 1, csData: []byte("fsm"), csReply: reply})
		gomega.Expect(<-reply).To(gomega.Succeed())

		snap, err := node.LatestSnapshot()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(snap.Learners).To(gomega.Equal(map[string]string{"n2": "addr2"}))
	})

	ginkgo.It("preserves learner metadata from InstallSnapshot", func(ginkgo.SpecContext) {
		node, err := NewNode(Config{ID: "n1", Peers: []string{"n2"}})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		node.Start()
		ginkgo.DeferCleanup(node.Stop)
		go func(node *Node) {
			for range node.ApplyCh() {
			}
		}(node)

		reply := node.HandleInstallSnapshot(&InstallSnapshotArgs{
			Term:              1,
			LeaderID:          "leader",
			LastIncludedIndex: 5,
			LastIncludedTerm:  1,
			Configuration:     []string{"n1"},
			Learners:          map[string]string{"n3": "addr3"},
			Data:              []byte("snapshot"),
		})
		gomega.Expect(reply.Term).To(gomega.Equal(uint64(1)))

		gomega.Eventually(func() bool {
			cfg := node.rs.Load().config
			return cfg.isLearner("n3") && cfg.learners["n3"] == "addr3"
		}, time.Second, 10*time.Millisecond).Should(gomega.BeTrue())
	}, ginkgo.NodeTimeout(5*time.Second))

	ginkgo.It("recovers a persisted snapshot from Badger", func(ginkgo.SpecContext) {
		dir, err := os.MkdirTemp("", "raft-snapshot-recovery-*")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(os.RemoveAll, dir)

		var snapshotIdx uint64
		var snapshotTerm uint64
		{
			db, closeDB, err := openRecoveryDB(dir)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			logStore, err := newBadgerLogStore(db, []byte("raft/v2/log/"))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			stable, err := newBadgerStableStore(db, []byte("raft/v2/hardstate/"))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			snapStore, err := newBadgerSnapshotStore(db, []byte("raft/v2/snap/"))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			node, err := NewNode(Config{
				ID:            "n1",
				LogStore:      logStore,
				StableStore:   stable,
				SnapshotStore: snapStore,
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			node.Start()
			go func(node *Node) {
				for range node.ApplyCh() {
				}
			}(node)
			gomega.Expect(waitFor(time.Second, func() bool { return node.IsLeader() })).To(gomega.Succeed())

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			for i := 1; i <= 10; i++ {
				_, err := node.ProposeWait(ctx, []byte(fmt.Sprintf("cmd-%d", i)))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			gomega.Expect(waitFor(2*time.Second, func() bool {
				return node.CommittedIndex() >= 10
			})).To(gomega.Succeed())

			gomega.Expect(node.CreateSnapshot(5, []byte("fsm-at-5"))).To(gomega.Succeed())
			snap, err := node.LatestSnapshot()
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			snapshotIdx = snap.LastIncludedIndex
			snapshotTerm = snap.LastIncludedTerm

			node.Stop()
			closeDB()
		}

		db, closeDB, err := openRecoveryDB(dir)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(closeDB)
		logStore, err := newBadgerLogStore(db, []byte("raft/v2/log/"))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		stable, err := newBadgerStableStore(db, []byte("raft/v2/hardstate/"))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapStore, err := newBadgerSnapshotStore(db, []byte("raft/v2/snap/"))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		node, err := NewNode(Config{
			ID:            "n1",
			LogStore:      logStore,
			StableStore:   stable,
			SnapshotStore: snapStore,
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		snap, err := node.LatestSnapshot()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(snap).NotTo(gomega.BeNil())
		gomega.Expect(snap.LastIncludedIndex).To(gomega.Equal(snapshotIdx))
		gomega.Expect(snap.LastIncludedTerm).To(gomega.Equal(snapshotTerm))
		gomega.Expect(snap.Data).To(gomega.Equal([]byte("fsm-at-5")))

		gomega.Expect(node.st.log.FirstIndex()).To(gomega.Equal(uint64(6)), "log FirstIndex must persist")
		gomega.Expect(node.st.log.LastIndex()).To(gomega.Equal(uint64(10)), "log LastIndex must persist")
		gomega.Expect(node.st.commitIndex).To(gomega.Equal(uint64(5)), "commitIndex must start at snapshot's LastIncludedIndex")
		term, err := node.st.log.TermAt(5)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(term).To(gomega.Equal(snapshotTerm), "TermAt(boundary) must equal snapshot term after restart")
	}, ginkgo.NodeTimeout(10*time.Second))

	ginkgo.Context("InstallSnapshot direct handling", func() {
		var node *Node
		var stopNode func()

		ginkgo.BeforeEach(func() {
			var err error
			node, err = NewNode(Config{
				ID:               "n1",
				Peers:            []string{"p1", "p2"},
				ElectionTimeout:  time.Hour,
				HeartbeatTimeout: testHeartbeat,
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			stopped := false
			stopNode = func() {
				if !stopped {
					node.Stop()
					stopped = true
				}
			}
			ginkgo.DeferCleanup(stopNode)
		})

		ginkgo.It("installs a follower snapshot and resets log state", func(ginkgo.SpecContext) {
			seedLogEntries(node, []LogEntry{
				{Term: 1, Index: 1, Command: []byte("old-A")},
				{Term: 1, Index: 2, Command: []byte("old-B")},
			})
			node.st.currentTerm = 1
			node.rs.Store(node.st.snapshot())

			node.Start()
			applied := make(chan LogEntry, 16)
			go func(node *Node) {
				for entry := range node.ApplyCh() {
					applied <- entry
				}
			}(node)

			reply := node.HandleInstallSnapshot(&InstallSnapshotArgs{
				Term:              5,
				LeaderID:          "leader",
				LastIncludedIndex: 100,
				LastIncludedTerm:  4,
				Configuration:     []string{"n1", "p1", "p2"},
				Data:              []byte("snapshot-blob"),
			})
			gomega.Expect(reply.Term).To(gomega.Equal(uint64(5)), "reply.Term must reflect new term after step-up")

			var entry LogEntry
			gomega.Eventually(applied, 2*time.Second).Should(gomega.Receive(&entry))
			gomega.Expect(entry.Type).To(gomega.Equal(LogEntrySnapshot))
			gomega.Expect(entry.Index).To(gomega.Equal(uint64(100)))
			gomega.Expect(entry.Term).To(gomega.Equal(uint64(4)))
			gomega.Expect(entry.Command).To(gomega.Equal([]byte("snapshot-blob")))

			stopNode()
			gomega.Expect(node.st.log.FirstIndex()).To(gomega.Equal(uint64(101)), "FirstIndex must equal LastIncludedIndex+1")
			gomega.Expect(node.st.log.LastIndex()).To(gomega.Equal(uint64(100)), "log empty above boundary; LastIndex == FirstIndex-1")
			gomega.Expect(node.st.commitIndex).To(gomega.Equal(uint64(100)), "commitIndex == LastIncludedIndex")
			term, err := node.st.log.TermAt(100)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(term).To(gomega.Equal(uint64(4)))
			snap, err := node.LatestSnapshot()
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(snap).NotTo(gomega.BeNil())
			gomega.Expect(snap.LastIncludedIndex).To(gomega.Equal(uint64(100)))
		}, ginkgo.NodeTimeout(5*time.Second))

		ginkgo.It("rejects stale-term snapshots without changing state", func() {
			seedLogEntries(node, []LogEntry{{Term: 1, Index: 1, Command: []byte("A")}})
			node.st.currentTerm = 5
			node.rs.Store(node.st.snapshot())

			node.Start()
			go func(node *Node) {
				for range node.ApplyCh() {
				}
			}(node)

			reply := node.HandleInstallSnapshot(&InstallSnapshotArgs{
				Term:              4,
				LeaderID:          "old-leader",
				LastIncludedIndex: 100,
				LastIncludedTerm:  3,
				Data:              []byte("stale"),
			})
			gomega.Expect(reply.Term).To(gomega.Equal(uint64(5)), "reply.Term must report current higher term")
			gomega.Expect(node.Term()).To(gomega.Equal(uint64(5)))
			snap, _ := node.LatestSnapshot()
			gomega.Expect(snap).To(gomega.BeNil(), "no snapshot saved on stale-term reject")

			stopNode()
			gomega.Expect(node.st.log.FirstIndex()).To(gomega.Equal(uint64(1)), "log FirstIndex unchanged")
			gomega.Expect(node.st.log.LastIndex()).To(gomega.Equal(uint64(1)), "log LastIndex unchanged")
		})

		ginkgo.It("allows a fresher snapshot over a prior snapshot", func() {
			node.st.currentTerm = 1
			node.rs.Store(node.st.snapshot())
			node.Start()
			go func(node *Node) {
				for range node.ApplyCh() {
				}
			}(node)

			first := node.HandleInstallSnapshot(&InstallSnapshotArgs{
				Term:              1,
				LeaderID:          "leader",
				LastIncludedIndex: 50,
				LastIncludedTerm:  1,
				Data:              []byte("snap@50"),
			})
			gomega.Expect(first.Term).To(gomega.Equal(uint64(1)))

			second := node.HandleInstallSnapshot(&InstallSnapshotArgs{
				Term:              1,
				LeaderID:          "leader",
				LastIncludedIndex: 100,
				LastIncludedTerm:  2,
				Data:              []byte("snap@100"),
			})
			gomega.Expect(second.Term).To(gomega.Equal(uint64(1)))

			stopNode()
			gomega.Expect(node.st.log.FirstIndex()).To(gomega.Equal(uint64(101)), "FirstIndex must advance to second snapshot boundary+1")
			gomega.Expect(node.st.commitIndex).To(gomega.Equal(uint64(100)))
		})

		ginkgo.It("ignores stale snapshots without regressing compacted state", func() {
			node.st.currentTerm = 1
			node.rs.Store(node.st.snapshot())
			node.Start()
			go func(node *Node) {
				for range node.ApplyCh() {
				}
			}(node)

			node.HandleInstallSnapshot(&InstallSnapshotArgs{
				Term:              1,
				LeaderID:          "leader",
				LastIncludedIndex: 100,
				LastIncludedTerm:  2,
				Data:              []byte("fresh"),
			})

			reply := node.HandleInstallSnapshot(&InstallSnapshotArgs{
				Term:              1,
				LeaderID:          "leader",
				LastIncludedIndex: 50,
				LastIncludedTerm:  1,
				Data:              []byte("stale"),
			})
			gomega.Expect(reply.Term).To(gomega.Equal(uint64(1)))

			stopNode()
			gomega.Expect(node.st.log.FirstIndex()).To(gomega.Equal(uint64(101)), "stale install must not regress FirstIndex")
			gomega.Expect(node.st.commitIndex).To(gomega.Equal(uint64(100)), "stale install must not regress commitIndex")
		})

		ginkgo.It("skips redundant snapshots when the follower is already caught up", func() {
			seedLogEntries(node, []LogEntry{
				{Term: 1, Index: 1, Command: []byte("A")},
				{Term: 1, Index: 2, Command: []byte("B")},
				{Term: 1, Index: 3, Command: []byte("C")},
			})
			node.st.currentTerm = 1
			node.rs.Store(node.st.snapshot())

			node.Start()
			go func(node *Node) {
				for range node.ApplyCh() {
				}
			}(node)

			reply := node.HandleInstallSnapshot(&InstallSnapshotArgs{
				Term:              1,
				LeaderID:          "leader",
				LastIncludedIndex: 2,
				LastIncludedTerm:  1,
				Data:              []byte("redundant"),
			})
			gomega.Expect(reply.Term).To(gomega.Equal(uint64(1)))

			stopNode()
			gomega.Expect(node.st.log.FirstIndex()).To(gomega.Equal(uint64(1)), "log not truncated when snapshot is redundant")
			gomega.Expect(node.st.log.LastIndex()).To(gomega.Equal(uint64(3)), "log entries preserved")
			snap, _ := node.LatestSnapshot()
			gomega.Expect(snap).To(gomega.BeNil(), "no snapshot saved on redundancy path")
		})
	})

	ginkgo.It("sends InstallSnapshot when a follower falls behind compaction", func(ginkgo.SpecContext) {
		net := newMemNetwork()

		node1, err := NewNode(Config{
			ID:               "n1",
			Peers:            []string{"n2", "n3"},
			ElectionTimeout:  fastElectionTimeout,
			HeartbeatTimeout: testHeartbeat,
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		node2, err := NewNode(Config{
			ID:               "n2",
			Peers:            []string{"n1", "n3"},
			ElectionTimeout:  slowElectionTimeout,
			HeartbeatTimeout: testHeartbeat,
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		node3, err := NewNode(Config{
			ID:               "n3",
			Peers:            []string{"n1", "n2"},
			ElectionTimeout:  slowElectionTimeout,
			HeartbeatTimeout: testHeartbeat,
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		snapTransport := &snapshotCountingTransport{
			inner: net.Register("n1", node1),
			count: make(map[string]int),
		}
		node1.SetTransport(snapTransport)
		node3.SetTransport(net.Register("n3", node3))

		for _, node := range []*Node{node1, node3} {
			node.Start()
			ginkgo.DeferCleanup(node.Stop)
			go func(node *Node) {
				for range node.ApplyCh() {
				}
			}(node)
		}
		gomega.Expect(waitFor(2*time.Second, func() bool { return node1.IsLeader() })).To(gomega.Succeed())

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for i := 0; i < 10; i++ {
			_, err := node1.ProposeWait(ctx, []byte(fmt.Sprintf("cmd-%d", i)))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		gomega.Expect(waitFor(2*time.Second, func() bool {
			return node1.CommittedIndex() >= 11 && node3.CommittedIndex() >= 11
		})).To(gomega.Succeed())

		gomega.Expect(node1.CreateSnapshot(8, []byte("fsm@8"))).To(gomega.Succeed())

		node2Applied := make(chan LogEntry, 64)
		go func(node *Node) {
			for entry := range node.ApplyCh() {
				node2Applied <- entry
			}
		}(node2)
		node2.SetTransport(net.Register("n2", node2))
		node2.Start()
		ginkgo.DeferCleanup(node2.Stop)

		gomega.Eventually(func() bool {
			snapTransport.mu.Lock()
			defer snapTransport.mu.Unlock()
			return snapTransport.count["n2"] > 0
		}, 3*time.Second, 10*time.Millisecond).Should(gomega.BeTrue(), "leader did not send InstallSnapshot to n2")

		var snapshotEntry LogEntry
		gomega.Eventually(func() bool {
			select {
			case entry := <-node2Applied:
				if entry.Type == LogEntrySnapshot {
					snapshotEntry = entry
					return true
				}
				return false
			default:
				return false
			}
		}, 2*time.Second, 10*time.Millisecond).Should(gomega.BeTrue(), "n2 did not receive LogEntrySnapshot on applyCh")
		gomega.Expect(snapshotEntry.Index).To(gomega.Equal(uint64(8)))

		gomega.Expect(waitFor(2*time.Second, func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			_, err := node1.ProposeWait(ctx, []byte("post-snapshot"))
			return err == nil && node2.CommittedIndex() >= node1.CommittedIndex()-1
		})).To(gomega.Succeed(), "n2 did not catch up post-InstallSnapshot")
	}, ginkgo.NodeTimeout(12*time.Second))
})

func TestBadgerSnapshotStore_RoundTripsCanonicalMetadata(t *testing.T) {
	g := gomega.NewWithT(t)
	db, err := badger.Open(badger.DefaultOptions(t.TempDir()).WithLogger(nil))
	g.Expect(err).NotTo(gomega.HaveOccurred())
	t.Cleanup(func() { g.Expect(db.Close()).To(gomega.Succeed()) })

	store, err := NewBadgerSnapshotStore(db, []byte("raft/v2/snap/"))
	g.Expect(err).NotTo(gomega.HaveOccurred())
	in := &Snapshot{
		Index:                12,
		Term:                 3,
		Servers:              []Server{{ID: "old-a", Suffrage: Voter}, {ID: "learner-a", Suffrage: NonVoter}},
		FormatVersion:        99,
		JointPhase:           JointEntering,
		JointOldVoters:       []string{"old-a", "old-b"},
		JointNewVoters:       []string{"old-a", "old-c"},
		JointEnterIndex:      11,
		JointManagedLearners: []string{"learner-a"},
		Data:                 []byte("fsm"),
	}

	g.Expect(store.Save(in)).To(gomega.Succeed())
	out, err := store.Latest()
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(out).NotTo(gomega.BeNil())
	g.Expect(out.Index).To(gomega.Equal(uint64(12)))
	g.Expect(out.Term).To(gomega.Equal(uint64(3)))
	g.Expect(out.Servers).To(gomega.Equal([]Server{{ID: "old-a", Suffrage: Voter}, {ID: "learner-a", Suffrage: NonVoter}}))
	g.Expect(out.FormatVersion).To(gomega.Equal(uint8(99)))
	g.Expect(out.JointPhase).To(gomega.Equal(JointEntering))
	g.Expect(out.JointOldVoters).To(gomega.Equal([]string{"old-a", "old-b"}))
	g.Expect(out.JointNewVoters).To(gomega.Equal([]string{"old-a", "old-c"}))
	g.Expect(out.JointEnterIndex).To(gomega.Equal(uint64(11)))
	g.Expect(out.JointManagedLearners).To(gomega.Equal([]string{"learner-a"}))
	g.Expect(out.Data).To(gomega.Equal([]byte("fsm")))
}

func TestBadgerSnapshotStore_RoundTripsLargeSnapshotData(t *testing.T) {
	g := gomega.NewWithT(t)
	db, err := badger.Open(badgerutil.SmallOptions(t.TempDir()))
	g.Expect(err).NotTo(gomega.HaveOccurred())
	t.Cleanup(func() { g.Expect(db.Close()).To(gomega.Succeed()) })

	store, err := NewBadgerSnapshotStore(db, []byte("raft/v2/snap/"))
	g.Expect(err).NotTo(gomega.HaveOccurred())

	data := make([]byte, 17<<20)
	for i := range data {
		data[i] = byte(i)
	}
	in := &Snapshot{
		LastIncludedIndex: 12,
		LastIncludedTerm:  3,
		Configuration:     []string{"n1"},
		Data:              data,
	}

	g.Expect(store.Save(in)).To(gomega.Succeed())
	out, err := store.Latest()
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(out).NotTo(gomega.BeNil())
	g.Expect(out.LastIncludedIndex).To(gomega.Equal(in.LastIncludedIndex))
	g.Expect(out.LastIncludedTerm).To(gomega.Equal(in.LastIncludedTerm))
	g.Expect(out.Configuration).To(gomega.Equal(in.Configuration))
	g.Expect(out.Data).To(gomega.Equal(data))
}

func TestBadgerSnapshotStore_ReplacingLargeSnapshotRemovesOldChunks(t *testing.T) {
	g := gomega.NewWithT(t)
	db, err := badger.Open(badgerutil.SmallOptions(t.TempDir()))
	g.Expect(err).NotTo(gomega.HaveOccurred())
	t.Cleanup(func() { g.Expect(db.Close()).To(gomega.Succeed()) })

	store, err := newBadgerSnapshotStore(db, []byte("raft/v2/snap/"))
	g.Expect(err).NotTo(gomega.HaveOccurred())

	large := &Snapshot{
		LastIncludedIndex: 12,
		LastIncludedTerm:  3,
		Configuration:     []string{"n1"},
		Data:              make([]byte, 17<<20),
	}
	g.Expect(store.Save(large)).To(gomega.Succeed())
	oldChunkKey := store.chunkKey(snapshotChunkID(large), 0)

	err = db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(oldChunkKey)
		return err
	})
	g.Expect(err).NotTo(gomega.HaveOccurred())

	small := &Snapshot{
		LastIncludedIndex: 13,
		LastIncludedTerm:  3,
		Configuration:     []string{"n1"},
		Data:              []byte("small"),
	}
	g.Expect(store.Save(small)).To(gomega.Succeed())

	err = db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(oldChunkKey)
		return err
	})
	g.Expect(err).To(gomega.MatchError(badger.ErrKeyNotFound))
}

// snapshotCountingTransport wraps a Transport and counts SendInstallSnapshot
// calls per peer. AE/RV pass through transparently. Used to assert the
// leader actually invoked the snapshot RPC (not merely the AE path).
type snapshotCountingTransport struct {
	inner Transport
	mu    sync.Mutex
	count map[string]int
}

func (s *snapshotCountingTransport) SendRequestVote(peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	return s.inner.SendRequestVote(peer, args)
}

func (s *snapshotCountingTransport) SendAppendEntries(peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	return s.inner.SendAppendEntries(peer, args)
}

func (s *snapshotCountingTransport) SendInstallSnapshot(peer string, args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	s.mu.Lock()
	if s.count == nil {
		s.count = make(map[string]int)
	}
	s.count[peer]++
	s.mu.Unlock()
	return s.inner.SendInstallSnapshot(peer, args)
}

func (s *snapshotCountingTransport) SendTimeoutNow(peer string, args *TimeoutNowArgs) (*TimeoutNowReply, error) {
	return s.inner.SendTimeoutNow(peer, args)
}
