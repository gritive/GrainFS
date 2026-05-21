package e2e

import (
	"github.com/onsi/ginkgo/v2"
)

// Iceberg REST Catalog endpoints that are implemented by GrainFS but lack
// explicit e2e coverage today. PIt entries are the [TODO:e2e] sentinel —
// grep "[TODO:e2e]" to enumerate pending coverage gaps. Each gap exists on
// both single-node and 4-node-cluster topologies; parity is the default.
var _ = ginkgo.Describe("Iceberg namespace and table skeletons", func() {
	for _, tc := range []struct {
		name string
		mk   func() *icebergTarget
	}{
		{name: "SingleNode", mk: func() *icebergTarget { return newSingleNodeIcebergTarget(ginkgo.GinkgoTB()) }},
		{name: "Cluster3Node", mk: func() *icebergTarget { return newSharedClusterIcebergTarget(ginkgo.GinkgoTB()) }},
	} {
		tc := tc
		ginkgo.Context(tc.name, func() {
			var tgt *icebergTarget
			_ = tgt

			ginkgo.BeforeEach(func() {
				tgt = tc.mk()
			})

			runIcebergNamespaceTableSkeletonCases(func() *icebergTarget { return tgt })
		})
	}
})

func runIcebergNamespaceTableSkeletonCases(getTgt func() *icebergTarget) {
	_ = getTgt

	ginkgo.PIt("[TODO:e2e] LoadNamespaceMetadata via HEAD returns 200 for existing namespace", func() {
		// HEAD /v1/namespaces/:namespace — existence check variant.
		// Should respond 200 for present namespace, 404 for missing.
	})

	ginkgo.PIt("[TODO:e2e] DropNamespace removes an empty namespace", func() {
		// DELETE /v1/namespaces/:namespace — verify success then HEAD 404.
		// Should reject non-empty namespace per Iceberg REST spec.
	})

	ginkgo.PIt("[TODO:e2e] LoadTable via HEAD returns 200 for existing table", func() {
		// HEAD /v1/namespaces/:ns/tables/:tbl — existence check variant.
		// Should respond 200 for present table, 404 for missing.
	})

	ginkgo.PIt("[TODO:e2e] CommitTransaction performs multi-table MVCC commit", func() {
		// POST /v1/transactions/commit — multi-table atomic commit.
		// Distinct from per-table UpdateTable; covers MVCC conflict path.
	})

	ginkgo.PIt("[TODO:e2e] DropWarehouse removes an empty warehouse", func() {
		// DELETE /v1/warehouses/:warehouse — verify success and that
		// follow-up ConfigEndpoint for the warehouse fails as expected.
	})

	// Modifier variants.
	ginkgo.PIt("[TODO:e2e] CreateTable supports stage-create / property-only / partition-spec", func() {
		// Three CreateTable variants share the endpoint but exercise
		// different code paths: stage-only (no commit), property update
		// without schema change, and explicit partition-spec install.
	})

	ginkgo.PIt("[TODO:e2e] Iceberg path encoding handles dots, multi-level, and special chars", func() {
		// Namespace names like "a.b" or "team/data" must round-trip through
		// the catalog encoder without collapsing hierarchy.
	})
}
