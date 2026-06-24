package e2e

import (
	"github.com/onsi/ginkgo/v2"
)

// `grainfs config` CLI subcommands lacking e2e coverage.
// Config CLI calls /v1/config{,/:key}. Dual context — surface is identical
// across topologies.
var _ = ginkgo.Describe("Config CLI skeletons", func() {
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

			runConfigCLISkeletonCases(func() s3Target { return tgt })
		})
	}
})

func runConfigCLISkeletonCases(getTgt func() s3Target) {
	_ = getTgt

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
