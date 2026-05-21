package e2e

import (
	"github.com/onsi/ginkgo/v2"
)

// `grainfs volume` subcommands without e2e coverage: stat, resize, recalculate.
// Volume CRUD/snapshot/IO are covered elsewhere (volume_cli_test.go).
var _ = ginkgo.Describe("Volume CLI skeletons", func() {
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

			runVolumeCLISkeletonCases(func() s3Target { return tgt })
		})
	}
})

func runVolumeCLISkeletonCases(getTgt func() s3Target) {
	_ = getTgt

	ginkgo.PIt("[TODO:e2e] volume stat reports allocation and usage", func() {
		// `grainfs volume stat <name>` — verify the reported size, used,
		// and free bytes reconcile with what `volume info` and writes show.
	})

	ginkgo.PIt("[TODO:e2e] volume resize grows the logical size", func() {
		// `grainfs volume resize <name> --size <new>` — verify the new size
		// is reflected in subsequent stat, and that data below the prior
		// boundary remains readable.
	})

	ginkgo.PIt("[TODO:e2e] volume recalculate refreshes usage accounting", func() {
		// `grainfs volume recalculate <name>` — after a mix of writes and
		// deletes, recalculate must converge `used` with the scanner's
		// authoritative value.
	})
}
