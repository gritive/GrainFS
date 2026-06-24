package e2e

import (
	"github.com/onsi/ginkgo/v2"
)

// Data-plane HTTP admin endpoints (under /api/*) without a CLI wrapper.
// These are reachable by operators directly via HTTP and are user-facing
// per Q1 (UI/dashboard surface). Dual context — endpoint surface is
// identical across topologies.
var _ = ginkgo.Describe("Admin HTTP endpoint skeletons", func() {
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

			runAdminHTTPEndpointSkeletonCases(func() s3Target { return tgt })
		})
	}
})

func runAdminHTTPEndpointSkeletonCases(getTgt func() s3Target) {
	_ = getTgt

	ginkgo.PIt("[TODO:e2e] GET /api/admin/alerts/status returns alert subsystem state", func() {
		// Verify the JSON shape, enabled flag, and recent-failure counter.
	})

	ginkgo.PIt("[TODO:e2e] POST /api/admin/alerts/resend re-emits the last failed alert", func() {
		// After synthesizing a failed alert, verify resend returns 200 and
		// the alert sink observes a duplicate.
	})

	ginkgo.PIt("[TODO:e2e] GET /api/cluster/lifecycle/status reports the worker phase", func() {
		// Lifecycle worker phases — verify expected phase rotation under
		// idle (paused / scanning / sleeping) and that the returned ts is fresh.
	})
}
