package e2e

import (
	"testing"

	"github.com/onsi/ginkgo/v2"
)

// 9P2000.L WalkGetAttr message — combined Twalk+Tgetattr atomic op. GrainFS
// embeds p9.DefaultWalkGetAttr, so the path is exercised implicitly by kernel
// 9P clients in tests/9p_colima, but no explicit wire-level e2e exists.
var _ = ginkgo.Describe("P9 WalkGetAttr skeleton", ginkgo.Label("p9"), func() {
	for _, tc := range []struct {
		name string
		mk   func(testing.TB) *p9Target
	}{
		{name: "SingleNode", mk: newSingleNodeP9Target},
		{name: "Cluster3Node", mk: newClusterP9Target},
	} {
		tc := tc
		ginkgo.Context(tc.name, func() {
			var tgt *p9Target
			_ = tgt

			ginkgo.BeforeEach(func() {
				tgt = tc.mk(ginkgo.GinkgoTB())
			})

			runP9WalkGetAttrSkeletonCases(func() *p9Target { return tgt })
		})
	}
})

func runP9WalkGetAttrSkeletonCases(getTgt func() *p9Target) {
	_ = getTgt

	ginkgo.PIt("[TODO:e2e] WalkGetAttr returns QIDs and attrs in a single op", func() {
		// Compose a P9 WalkGetAttr against a multi-component path (bucket/key).
		// Verify QIDs returned for each component AND that the final attr
		// (mode/atime/mtime/size) matches independent Tgetattr on the leaf.
		// Also assert error paths: ENOENT for missing leaf, ENOTDIR for
		// non-directory intermediate.
	})
}
