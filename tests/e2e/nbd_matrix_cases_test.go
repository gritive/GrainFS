package e2e

import "github.com/onsi/ginkgo/v2"

func runNBDCases(getTgt func() *nbdTarget) {
	ginkgo.It("round-trips writes and reads", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		device := tgt.uniqueDevice(t, "rw-roundtrip", 4*1024*1024)

		// Cluster fixture requires an explicit admin grant for the internal
		// volume bucket. Volume creation itself owns the bucket lifecycle.
		if tgt.isCluster && tgt.cluster != nil {
			tgt.cluster.GrantAdminOnBuckets("__grainfs_volumes")
		}

		client := dialE2ENBD(t, tgt.nbdAddr(0), device)
		ginkgo.DeferCleanup(client.Close)

		body := []byte("nbd-matrix-roundtrip-payload")
		client.WriteAt(t, 0, body)
		client.Flush(t)
		requireNBDReadEventually(t, client, 0, body)
	})
}

// TestNBDMatrixE2E runs the NBD matrix case set against the single-node and
// the shared 4-node cluster fixtures, mirroring the TestBucketsE2E dual-target
// convention so the same NBD client-side behaviors are exercised against both
// deployment shapes.
var _ = ginkgo.Describe("NBD matrix", func() {
	for _, tc := range []struct {
		name string
		mk   func() *nbdTarget
	}{
		{name: "SingleNode", mk: func() *nbdTarget { return newSingleNodeNBDTarget(ginkgo.GinkgoTB()) }},
		{name: "Cluster4Node", mk: func() *nbdTarget { return newSharedClusterNBDTarget(ginkgo.GinkgoTB()) }},
	} {
		tc := tc
		ginkgo.Context(tc.name, func() {
			var tgt *nbdTarget

			ginkgo.BeforeEach(func() {
				tgt = tc.mk()
			})

			runNBDCases(func() *nbdTarget { return tgt })
		})
	}
})
