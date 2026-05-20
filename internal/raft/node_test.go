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
