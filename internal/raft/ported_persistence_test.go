package raft

// ported_persistence_test.go: after-Stop RPC safety tests ported from
// internal/raft/persistence_test.go (PR 19 batch 1).
//
// v1 equivalents:
//   TestNode_RequestVoteAfterCloseDoesNotPersist
//   TestNode_AppendEntriesAfterCloseDoesNotPersist
//   TestNode_InstallSnapshotAfterCloseDoesNotPersist
//
// Mapping: v1 node.Close() → v2 node.Stop().
// v2 already documents this behaviour in node.go:311 ("matches v1's
// stopped-node behaviour"). These tests provide explicit regression coverage.

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Stopped-node RPC safety", func() {
	var node *Node

	ginkgo.BeforeEach(func() {
		var err error
		node, err = NewNode(Config{ID: "A", Peers: []string{"B"}})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		node.Start()
		go func(node *Node) {
			for range node.ApplyCh() {
			}
		}(node)
		node.Stop()
	})

	ginkgo.It("returns a RequestVote reply after Stop without panicking", func() {
		gomega.Expect(func() {
			reply := node.HandleRequestVote(&RequestVoteArgs{
				Term:        2,
				CandidateID: "B",
			})
			gomega.Expect(reply).NotTo(gomega.BeNil())
			gomega.Expect(reply.VoteGranted).To(gomega.BeFalse())
		}).NotTo(gomega.Panic())
	})

	ginkgo.It("returns an AppendEntries reply after Stop without panicking", func() {
		gomega.Expect(func() {
			reply := node.HandleAppendEntries(&AppendEntriesArgs{
				Term:     2,
				LeaderID: "B",
			})
			gomega.Expect(reply).NotTo(gomega.BeNil())
			gomega.Expect(reply.Success).To(gomega.BeFalse())
		}).NotTo(gomega.Panic())
	})

	ginkgo.It("returns an InstallSnapshot reply after Stop without panicking", func() {
		gomega.Expect(func() {
			reply := node.HandleInstallSnapshot(&InstallSnapshotArgs{
				Term:              2,
				LeaderID:          "B",
				LastIncludedIndex: 10,
				LastIncludedTerm:  2,
				Data:              []byte("snapshot"),
				Configuration:     []string{"A"},
			})
			gomega.Expect(reply).NotTo(gomega.BeNil())
		}).NotTo(gomega.Panic())
	})

	ginkgo.It("allows Stop to be called multiple times", func() {
		node, err := NewNode(Config{ID: "n1"})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		node.Start()
		go func() {
			for range node.ApplyCh() {
			}
		}()

		gomega.Expect(waitFor(time.Second, node.IsLeader)).To(gomega.Succeed())
		gomega.Expect(func() {
			node.Stop()
			node.Stop()
			node.Stop()
		}).NotTo(gomega.Panic())
	})
})
