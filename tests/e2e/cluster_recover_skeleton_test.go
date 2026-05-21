package e2e

import (
	"github.com/onsi/ginkgo/v2"
)

// `grainfs recover cluster` subcommands handle offline disaster recovery
// after raft majority loss. Cluster-only by nature: single-node has no
// quorum to recover. Per docs/operators/recover-cluster.md.
var _ = ginkgo.Describe("Cluster recover CLI skeletons", func() {
	// Cluster-only — topology-inherent exception (Q5).
	ginkgo.Context("Cluster4Node", func() {
		var tgt s3Target
		_ = tgt

		ginkgo.BeforeEach(func() {
			tgt = newSharedClusterS3Target(ginkgo.GinkgoTB())
		})

		runClusterRecoverSkeletonCases(func() s3Target { return tgt })
	})
})

func runClusterRecoverSkeletonCases(getTgt func() s3Target) {
	_ = getTgt

	ginkgo.PIt("[TODO:e2e] recover cluster plan reports the proposed peer set", func() {
		// `grainfs recover cluster plan --data <dir>` — offline command.
		// Should parse the local raft state and print the recovery plan
		// without mutating anything. Verify exit code 0 and output shape.
	})

	ginkgo.PIt("[TODO:e2e] recover cluster execute writes the new raft membership", func() {
		// `grainfs recover cluster execute --data <dir> --confirm` — offline.
		// Verify that after execute, the node boots successfully into the
		// reduced membership and quorum is regained.
	})

	ginkgo.PIt("[TODO:e2e] recover cluster verify confirms the post-recovery state", func() {
		// `grainfs recover cluster verify --data <dir>` — offline read-only.
		// Verify it reports OK only after a successful execute, and reports
		// inconsistency when raft state diverges from the planned membership.
	})
}
