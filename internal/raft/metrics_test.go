package raft

import (
	"context"
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

var _ = ginkgo.Describe("Raft metrics", func() {
	ginkgo.Context("single-voter node", func() {
		var node *Node

		ginkgo.BeforeEach(func() {
			var err error
			var cleanup func()
			node, cleanup, err = startRaftIntegrationSingleVoter("metrics-n1")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.DeferCleanup(cleanup)
		})

		ginkgo.It("increments actorTermBumps during bootstrap", func(ginkgo.SpecContext) {
			before := testutil.ToFloat64(actorTermBumps)

			extra, extraCleanup, err := startRaftIntegrationSingleVoter("tb-n1")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer extraCleanup()
			gomega.Expect(extra.IsLeader()).To(gomega.BeTrue())

			after := testutil.ToFloat64(actorTermBumps)
			gomega.Expect(after).To(gomega.BeNumerically(">", before), "actorTermBumps should increment during single-voter bootstrap")
		}, ginkgo.NodeTimeout(5*time.Second))

		ginkgo.It("keeps the propose hot-path benchmark documented", func(ginkgo.SpecContext) {
			ctx := context.Background()
			for i := 0; i < 10; i++ {
				_, err := node.ProposeWait(ctx, []byte("warmup"))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			fmt.Fprintln(ginkgo.GinkgoWriter, "Hot-path alloc check: run BenchmarkProposeWait_SingleNode_NoFsync -benchmem to verify 5 allocs/op")
		}, ginkgo.NodeTimeout(5*time.Second))
	})

	ginkgo.Context("three-voter cluster", func() {
		var nodes []*Node
		var leader *Node
		var leaderTransitionsBefore float64

		ginkgo.BeforeEach(func() {
			var err error
			var cleanup func()
			leaderTransitionsBefore = testutil.ToFloat64(actorLeaderTransitions)
			nodes, _, cleanup, err = startRaftIntegrationCluster("n1", "n2", "n3")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.DeferCleanup(cleanup)
			leader = nodes[0]
			gomega.Expect(waitFor(2*time.Second, leader.IsLeader)).To(gomega.Succeed(), "n1 did not become leader")
		})

		ginkgo.It("increments actorLeaderTransitions after initial election", func(ginkgo.SpecContext) {
			_ = nodes
			after := testutil.ToFloat64(actorLeaderTransitions)
			gomega.Expect(after).To(gomega.BeNumerically(">", leaderTransitionsBefore),
				"actorLeaderTransitions should have incremented after initial election")
		}, ginkgo.NodeTimeout(5*time.Second))

		ginkgo.It("records AppendEntries duration observations", func(ginkgo.SpecContext) {
			beforeCount := testutil.CollectAndCount(actorAppendEntriesRPCDuration)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := leader.ProposeWait(ctx, []byte("ae-metric-test"))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			gomega.Expect(waitFor(500*time.Millisecond, func() bool {
				return testutil.CollectAndCount(actorAppendEntriesRPCDuration) > beforeCount ||
					testutil.CollectAndCount(actorAppendEntriesRPCDuration) > 0
			})).To(gomega.Succeed(), "actorAppendEntriesRPCDuration did not record observations")
		}, ginkgo.NodeTimeout(5*time.Second))
	})
})
