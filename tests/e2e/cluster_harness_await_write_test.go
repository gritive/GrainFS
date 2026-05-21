package e2e

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// TestClusterAwaitWriteFromNonOwnerE2E verifies that AwaitWriteFromNonOwner
// returns nil once a write against the cluster succeeds from at least one
// non-leader peer. Cluster-only by nature: requires ≥2 peers. Runs on the
// shared 4-node cluster fixture; no single-node analogue.
var _ = ginkgo.Describe("Cluster await write", func() {
	ginkgo.Context("Cluster4Node", func() {
		var tgt s3Target

		ginkgo.BeforeEach(func() {
			tgt = newSharedClusterS3Target(ginkgo.GinkgoTB())
		})

		runAwaitWriteFromNonOwnerCases(func() s3Target { return tgt })
	})
})

func runAwaitWriteFromNonOwnerCases(getTgt func() s3Target) {
	ginkgo.It("succeeds from at least one non-owner peer", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		gomega.Expect(tgt.isCluster).To(gomega.BeTrue(), "AwaitWriteFromNonOwner requires cluster fixture")
		bucket := tgt.uniqueBucket(t, "awaitprobe")
		gomega.Expect(tgt.cluster.AwaitWriteFromNonOwner(bucket, "leader-check", 15*time.Second)).To(gomega.Succeed())
	})
}
