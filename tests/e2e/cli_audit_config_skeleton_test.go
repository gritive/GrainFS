package e2e

import (
	"github.com/onsi/ginkgo/v2"
)

// `grainfs audit` and `grainfs config` CLI subcommands lacking e2e coverage.
// Audit query CLI calls into POST /v1/audit/query; config CLI calls
// /v1/config{,/:key}. Dual context — surface is identical across topologies.
var _ = ginkgo.Describe("Audit and config CLI skeletons", func() {
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

			runAuditConfigCLISkeletonCases(func() s3Target { return tgt })
		})
	}
})

func runAuditConfigCLISkeletonCases(getTgt func() s3Target) {
	_ = getTgt

	// audit subcommands.
	ginkgo.PIt("[TODO:e2e] audit query returns rows matching a filter", func() {
		// `grainfs audit query --since <ts> --action s3:GetObject` — verify
		// JSON shape and that emitted audit events appear in the result.
	})

	ginkgo.PIt("[TODO:e2e] audit recent-denies surfaces recent AccessDenied rows", func() {
		// `grainfs audit recent-denies` — after a synthetic deny, the row
		// must appear in the listing.
	})

	ginkgo.PIt("[TODO:e2e] audit by-sa filters by service-account id", func() {
		// `grainfs audit by-sa <saID>` — verify scoping returns only that
		// SA's events.
	})

	ginkgo.PIt("[TODO:e2e] audit by-request-id retrieves a single request trace", func() {
		// `grainfs audit by-request-id <rid>` — verify retrieval round-trip
		// after capturing a known request-id from an S3 response.
	})

	// config subcommands.
	ginkgo.PIt("[TODO:e2e] config list enumerates all cluster config keys", func() {
		// `grainfs config list` — verify JSON shape and a known default key.
	})

	ginkgo.PIt("[TODO:e2e] config get returns a single key value", func() {
		// `grainfs config get <key>` — verify default value before set,
		// updated value after set.
	})

	ginkgo.PIt("[TODO:e2e] config set persists a key/value", func() {
		// `grainfs config set <key> <value>` — verify persistence across
		// reads and (in cluster) follower observation.
	})

	ginkgo.PIt("[TODO:e2e] config unset reverts a key to default", func() {
		// `grainfs config unset <key>` — verify the value returns to default.
	})
}
