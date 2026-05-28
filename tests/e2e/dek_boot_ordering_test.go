package e2e

import (
	"context"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("R1 DEK boot ordering", ginkgo.Ordered, func() {
	ginkgo.Context("3-node cluster join then restart (encryption ON)", func() {
		var c *e2eCluster
		var leaderIdx int
		const bucket = "r1boot"

		ginkgo.BeforeAll(func() {
			// startE2ECluster brings up genesis + joiners with a shared encryption
			// DEK metadata is bootstrapped from the data directory, so at-rest encryption is ON.
			// It registers DeferCleanup(c.Stop) internally.
			c = startE2ECluster(ginkgo.GinkgoTB(), e2eClusterOptions{Nodes: 3})
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			var err error
			leaderIdx, err = c.EnsureBucketWritable(ctx, bucket, 60*time.Second)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		})

		ginkgo.It("a joiner booted without deadlocking on WaitDEKReady", func() {
			// Reaching a writable cluster means each joiner populated its keeper via
			// meta-raft catch-up BEFORE the gate (no deadlock) and opened its
			// DEK-sealed logical WAL after the gate.
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			gomega.Expect(tryPutObject(ctx, c.S3Client(2), bucket, "viaJoiner", []byte("hello"))).To(gomega.Succeed())
			got, err := getObjectBytes(ctx, c.S3Client(2), bucket, "viaJoiner")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(got).To(gomega.Equal([]byte("hello")))
		})

		ginkgo.It("restarts the written-through node and reopens its logical WAL", func() {
			t := ginkgo.GinkgoTB()
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()
			// Pick a non-leader victim (mirrors cluster_harness_kill_test) and write
			// THROUGH it so it holds the logical/PITR WAL segments. Killing+restarting
			// that same node makes "decrypt existing WAL on restart" non-vacuous: the
			// restarted node must reopen the (encryption-ON) logical WAL after
			// WaitDEKReady to serve subsequent traffic.
			victim := (leaderIdx + 1) % 3
			gomega.Expect(tryPutObject(ctx, c.S3Client(victim), bucket, "persist", []byte("durable"))).To(gomega.Succeed())

			c.KillNode(victim)
			waitClusterSettled(t, c.httpURLs[leaderIdx])
			c.RestartNode(t, victim)
			// The restarted node must rejoin (reopened its logical WAL during boot,
			// which succeeds only if WaitDEKReady -> bootLogicalWALOpen completed).
			gomega.Eventually(func() bool {
				return nodeSettled(c.httpURLs[victim])
			}, 90*time.Second, 500*time.Millisecond).Should(gomega.BeTrue(), "restarted node never settled")

			// The prior object (in the reopened logical WAL) reads back through the
			// restarted node...
			gomega.Eventually(func() error {
				_, err := getObjectBytes(ctx, c.S3Client(victim), bucket, "persist")
				return err
			}, 30*time.Second, 2*time.Second).Should(gomega.Succeed())
			// ...and a subsequent write+read through the same node succeeds, proving
			// the reopened logical WAL is live, not just readable.
			gomega.Eventually(func() error {
				return tryPutObject(ctx, c.S3Client(victim), bucket, "afterRestart", []byte("fresh"))
			}, 30*time.Second, 2*time.Second).Should(gomega.Succeed())
			gomega.Eventually(func() ([]byte, error) {
				return getObjectBytes(ctx, c.S3Client(victim), bucket, "afterRestart")
			}, 30*time.Second, 2*time.Second).Should(gomega.Equal([]byte("fresh")))
		})
	})

	ginkgo.Context("single-node parity (1-node, encryption ON)", func() {
		var c *e2eCluster
		const bucket = "r1single"

		ginkgo.BeforeAll(func() {
			c = startE2ECluster(ginkgo.GinkgoTB(), e2eClusterOptions{Nodes: 1})
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			_, err := c.EnsureBucketWritable(ctx, bucket, 60*time.Second)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		})

		ginkgo.It("boots, round-trips, and recovers across restart", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			gomega.Expect(tryPutObject(ctx, c.S3Client(0), bucket, "k", []byte("x"))).To(gomega.Succeed())
			c.KillNode(0)
			c.RestartNode(ginkgo.GinkgoTB(), 0)
			gomega.Eventually(func() error {
				_, err := getObjectBytes(ctx, c.S3Client(0), bucket, "k")
				return err
			}, 60*time.Second, 2*time.Second).Should(gomega.Succeed())
		})
	})
})
