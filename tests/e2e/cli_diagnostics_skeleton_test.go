package e2e

import (
	"github.com/onsi/ginkgo/v2"
)

// Diagnostic CLI subcommands with no e2e coverage: iceberg config,
// nfs debug. These are read-only probes; dual context.
var _ = ginkgo.Describe("Diagnostics CLI skeletons", func() {
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

			runDiagnosticsCLISkeletonCases(func() s3Target { return tgt })
		})
	}
})

func runDiagnosticsCLISkeletonCases(getTgt func() s3Target) {
	_ = getTgt

	ginkgo.PIt("[TODO:e2e] iceberg config shows the iceberg client config", func() {
		// `grainfs iceberg config --endpoint <sock>` — verify endpoint URLs
		// and OAuth posture are reported as configured.
	})

	ginkgo.PIt("[TODO:e2e] nfs debug renders the NFS export internal state", func() {
		// `grainfs nfs debug <export> --endpoint <sock>` — verify state
		// fields (bucket binding, recent ops, last-error) render after
		// touching the export.
	})
}
