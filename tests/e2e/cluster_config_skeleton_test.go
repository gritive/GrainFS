package e2e

import (
	"github.com/onsi/ginkgo/v2"
)

// `grainfs cluster config` subcommands that are wired in the cobra tree but
// lack e2e coverage. Config exists on both topologies (mode is always
// "cluster"; single = 0 peers), so dual SingleNode/Cluster4Node context.
var _ = ginkgo.Describe("Cluster config CLI skeletons", func() {
	for _, tc := range []struct {
		name string
		mk   func() s3Target
	}{
		{name: "SingleNode", mk: newSingleNodeS3Target},
		{name: "Cluster4Node", mk: func() s3Target { return newSharedClusterS3Target(ginkgo.GinkgoTB()) }},
	} {
		tc := tc
		ginkgo.Context(tc.name, func() {
			var tgt s3Target
			_ = tgt

			ginkgo.BeforeEach(func() {
				tgt = tc.mk()
			})

			runClusterConfigSkeletonCases(func() s3Target { return tgt })
		})
	}
})

func runClusterConfigSkeletonCases(getTgt func() s3Target) {
	_ = getTgt

	ginkgo.PIt("[TODO:e2e] cluster config diff shows pending vs applied config", func() {
		// `grainfs cluster config diff --endpoint <sock>`
		// Should render the diff between local proposed config and the
		// quorum-applied snapshot. Verify output shape (JSON/text), and that
		// an unmodified config reports an empty diff.
	})

	ginkgo.PIt("[TODO:e2e] cluster config reset restores defaults", func() {
		// `grainfs cluster config reset --endpoint <sock> [--key <name>]`
		// Verify a previously-set value reverts after reset and that the
		// follower path observes the rollback within the hot-reload window.
	})
}
