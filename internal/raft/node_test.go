package raft

import (
	"bytes"
	"context"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Single-node actor", func() {
	var node *Node

	ginkgo.BeforeEach(func() {
		var err error
		node, err = NewNode(Config{ID: "n1"})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		node.Start()
		ginkgo.DeferCleanup(node.Stop)
		gomega.Expect(waitFor(time.Second, node.IsLeader)).To(gomega.Succeed())
	})

	ginkgo.It("round-trips a proposed command through ApplyCh", func(ginkgo.SpecContext) {
		gomega.Expect(node.State()).To(gomega.Equal(Leader))
		gomega.Expect(node.Term()).To(gomega.Equal(uint64(1)))
		gomega.Expect(node.IsLeader()).To(gomega.BeTrue())
		gomega.Expect(node.LeaderID()).To(gomega.Equal("n1"))
		gomega.Expect(node.CommittedIndex()).To(gomega.Equal(uint64(0)))

		cmd := []byte("hello")
		gomega.Expect(node.Propose(cmd)).To(gomega.Succeed())

		select {
		case entry := <-node.ApplyCh():
			gomega.Expect(bytes.Equal(entry.Command, cmd)).To(gomega.BeTrue())
			gomega.Expect(entry.Index).To(gomega.Equal(uint64(1)))
			gomega.Expect(entry.Term).To(gomega.Equal(uint64(1)))
		case <-time.After(time.Second):
			ginkgo.Fail("timeout waiting for ApplyCh entry")
		}

		gomega.Expect(node.CommittedIndex()).To(gomega.Equal(uint64(1)))
	}, ginkgo.NodeTimeout(5*time.Second))

	ginkgo.It("waits for commit and returns the log index", func(ginkgo.SpecContext) {
		go func() {
			for range node.ApplyCh() {
			}
		}()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		idx, err := node.ProposeWait(ctx, []byte("payload"))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(idx).To(gomega.Equal(uint64(1)))
	}, ginkgo.NodeTimeout(5*time.Second))
})

var _ = ginkgo.Describe("NewNode durability guards", func() {
	ginkgo.It("refuses boot when the log is compacted but no snapshot is present", func() {
		// A compacted log (FirstIndex > 1) implies a snapshot was once persisted
		// (CreateSnapshot saves before CompactBefore). If that snapshot is now
		// absent, the FSM state for [1, FirstIndex-1] is unreconstructable —
		// refuse boot rather than silently seed-fallback and replay a possibly-
		// uncommitted log tail.
		logStore := newMemLogStore()
		gomega.Expect(logStore.Append([]LogEntry{
			{Term: 1, Index: 1, Type: LogEntryCommand, Command: []byte("a")},
			{Term: 1, Index: 2, Type: LogEntryCommand, Command: []byte("b")},
			{Term: 1, Index: 3, Type: LogEntryCommand, Command: []byte("c")},
		})).To(gomega.Succeed())
		gomega.Expect(logStore.CompactBefore(2)).To(gomega.Succeed()) // FirstIndex -> 3
		gomega.Expect(logStore.FirstIndex()).To(gomega.BeNumerically(">", uint64(1)))

		_, err := NewNode(Config{
			ID:            "n1",
			LogStore:      logStore,
			SnapshotStore: newMemSnapshotStore(), // empty: snapshot absent
		})
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("compacted"))
	})

	ginkgo.It("boots normally with an uncompacted log and no snapshot (genuine genesis)", func() {
		// FirstIndex == 1 + no snapshot is the legitimate fresh-genesis case
		// (#685 solo restart): the guard must NOT fire here.
		logStore := newMemLogStore()
		gomega.Expect(logStore.Append([]LogEntry{
			{Term: 1, Index: 1, Type: LogEntryCommand, Command: []byte("a")},
		})).To(gomega.Succeed())
		gomega.Expect(logStore.FirstIndex()).To(gomega.Equal(uint64(1)))
		n, err := NewNode(Config{ID: "n1", LogStore: logStore, SnapshotStore: newMemSnapshotStore()})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(n).NotTo(gomega.BeNil())
	})
})

// waitFor polls cond until it returns true or the deadline elapses. Returns
// nil on success, ctx error on timeout. Used to bridge the brief async window
// between Start() and the actor's first readState publish.
func waitFor(d time.Duration, cond func() bool) error {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return nil
		}
		time.Sleep(time.Millisecond)
	}
	if cond() {
		return nil
	}
	return context.DeadlineExceeded
}
