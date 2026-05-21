package e2e

import (
	"github.com/onsi/ginkgo/v2"
)

// `grainfs bucket` subcommands with no e2e coverage today: info, policy
// {get,set,delete}, versioning {get,enable,suspend}. All run against the
// admin UDS; dual SingleNode/Cluster4Node since the surface is identical.
var _ = ginkgo.Describe("Bucket admin CLI skeletons", func() {
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

			runBucketAdminCLISkeletonCases(func() s3Target { return tgt })
		})
	}
})

func runBucketAdminCLISkeletonCases(getTgt func() s3Target) {
	_ = getTgt

	ginkgo.PIt("[TODO:e2e] bucket info renders bucket metadata", func() {
		// `grainfs bucket info <name> --endpoint <sock>` — verify creation
		// time, owner, versioning state, upstream binding (if any).
	})

	ginkgo.PIt("[TODO:e2e] bucket policy get returns the configured policy", func() {
		// `grainfs bucket policy get <name>` — empty for new bucket, returns
		// JSON document after set.
	})

	ginkgo.PIt("[TODO:e2e] bucket policy set installs a policy document", func() {
		// `grainfs bucket policy set <name> --file <path>` — verify the
		// policy takes effect (e.g., S3 GetObject denied per the rule).
	})

	ginkgo.PIt("[TODO:e2e] bucket policy delete removes the policy", func() {
		// `grainfs bucket policy delete <name>` — verify follow-up get
		// returns empty and prior denials no longer apply.
	})

	ginkgo.PIt("[TODO:e2e] bucket versioning get reports the current state", func() {
		// `grainfs bucket versioning get <name>` — Off / Enabled / Suspended.
	})

	ginkgo.PIt("[TODO:e2e] bucket versioning enable turns on versioning", func() {
		// `grainfs bucket versioning enable <name>` — verify state transition
		// and that subsequent PutObject creates a version.
	})

	ginkgo.PIt("[TODO:e2e] bucket versioning suspend preserves existing versions", func() {
		// `grainfs bucket versioning suspend <name>` — verify state and that
		// existing versions remain readable while new puts overwrite the
		// null version.
	})
}
