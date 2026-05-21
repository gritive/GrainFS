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

	ginkgo.PIt("[TODO:e2e] NBD_CMD_DISC issues graceful disconnect (transmission phase)", func() {
		// Currently only implicit close exercises this. Explicit test should
		// send NBD_CMD_DISC, then verify server tears down the session cleanly
		// (no error reply expected, connection closed by server).
	})

	ginkgo.PIt("[TODO:e2e] NBD_OPT_ABORT terminates handshake without committing to export", func() {
		// Client negotiates, then sends NBD_OPT_ABORT. Server should reply
		// with NBD_REP_ACK and close. No export should be selected.
	})

	ginkgo.PIt("[TODO:e2e] NBD_OPT_LIST returns the configured exports", func() {
		// Client sends NBD_OPT_LIST during negotiation; server replies with
		// NBD_REP_SERVER for each export then NBD_REP_ACK. Verify list shape
		// matches the exports configured by volume create.
	})

	// Modifier variants.
	ginkgo.PIt("[TODO:e2e] NBD_CMD_WRITE_ZEROES with NO_HOLE forces full write", func() {
		// Verify the NO_HOLE flag prevents the server from punching a hole;
		// follow-up BLOCK_STATUS must report the range as allocated.
	})

	ginkgo.PIt("[TODO:e2e] NBD_CMD_TRIM accepts partial / full / beyond-device ranges", func() {
		// Partial trim retains data outside the range; full trim zeroes
		// everything; beyond-device must error with EINVAL.
	})

	ginkgo.PIt("[TODO:e2e] NBD_CMD_FLUSH commits pending writes across multi-conn", func() {
		// With multi-conn negotiated, writes from connection A followed by
		// FLUSH from connection B must observe A's data persisted.
	})

	ginkgo.PIt("[TODO:e2e] NBD_CMD_BLOCK_STATUS reports allocated vs hole accurately", func() {
		// After WRITE then TRIM, BLOCK_STATUS must distinguish the data
		// region from the trimmed region with correct flag bits.
	})

	ginkgo.PIt("[TODO:e2e] structured reply chunks large READ into multiple OFFSET_DATA frames", func() {
		// READ of a size exceeding the structured-reply chunk threshold must
		// produce multiple OFFSET_DATA chunks reassembling to the original.
	})

	ginkgo.PIt("[TODO:e2e] multi-connection concurrent writes preserve append ordering", func() {
		// Concurrent writes to overlapping blocks from two connections must
		// not interleave bytes; final state matches one of the two orderings.
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
