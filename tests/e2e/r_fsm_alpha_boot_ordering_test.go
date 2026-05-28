package e2e

import (
	"context"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// R-FSM-α boot ordering smoke. The real ordering invariants live in the
// internal/serveruntime source-grep test (boot_phase_split_test.go). This file
// asserts the reorder did not regress end-to-end boot: a node (single or
// cluster) still serves S3 PUT/GET and survives a restart.
var _ = ginkgo.Describe("R-FSM-α boot ordering smoke", ginkgo.Ordered, func() {
	ginkgo.Context("single-node (encryption ON)", func() {
		var c *e2eCluster
		const bucket = "rfsmalpha1"

		ginkgo.BeforeAll(func() {
			c = startE2ECluster(ginkgo.GinkgoTB(), e2eClusterOptions{Nodes: 1})
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			_, err := c.EnsureBucketWritable(ctx, bucket, 60*time.Second)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		})

		ginkgo.It("PUT, restart, GET round-trips with matching bytes", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			payload := []byte("rfsm-alpha-single")
			gomega.Expect(tryPutObject(ctx, c.S3Client(0), bucket, "k", payload)).To(gomega.Succeed())

			c.KillNode(0)
			c.RestartNode(ginkgo.GinkgoTB(), 0)

			gomega.Eventually(func() ([]byte, error) {
				return getObjectBytes(ctx, c.S3Client(0), bucket, "k")
			}, 60*time.Second, 2*time.Second).Should(gomega.Equal(payload))
		})
	})

	ginkgo.Context("3-node cluster (encryption ON)", func() {
		var c *e2eCluster
		var leaderIdx int
		const bucket = "rfsmalpha3"

		ginkgo.BeforeAll(func() {
			c = startE2ECluster(ginkgo.GinkgoTB(), e2eClusterOptions{Nodes: 3})
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			var err error
			leaderIdx, err = c.EnsureBucketWritable(ctx, bucket, 60*time.Second)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		})

		ginkgo.It("PUT, restart non-leader, GET round-trips with matching bytes", func() {
			t := ginkgo.GinkgoTB()
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()
			payload := []byte("rfsm-alpha-cluster")
			victim := (leaderIdx + 1) % 3
			gomega.Expect(tryPutObject(ctx, c.S3Client(victim), bucket, "persist", payload)).To(gomega.Succeed())

			c.KillNode(victim)
			waitClusterSettled(t, c.httpURLs[leaderIdx])
			c.RestartNode(t, victim)
			gomega.Eventually(func() bool {
				return nodeSettled(c.httpURLs[victim])
			}, 90*time.Second, 500*time.Millisecond).Should(gomega.BeTrue(), "restarted node never settled")

			gomega.Eventually(func() ([]byte, error) {
				return getObjectBytes(ctx, c.S3Client(victim), bucket, "persist")
			}, 30*time.Second, 2*time.Second).Should(gomega.Equal(payload))
		})
	})
})
