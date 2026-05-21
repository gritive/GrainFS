package e2e

import (
	"testing"
	"time"

	"github.com/hugelgupf/p9/p9"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/stretchr/testify/require"
)

// Phase 0→2 P9 anon close exercises the NFS§B T12 / §9 T73 invariant
// over real 9P wire: an anon-bound 9P session that worked before the Phase
// 0→2 flip is rejected on the next anon-attach to a non-default bucket after
// the flip.
//
// SingleNode only — mrCluster always bootstraps admin (Phase 2 from boot), so
// there is no point where Phase 0 → 2 can be driven explicitly. Cluster3Node
// coverage requires either a noBootstrap mrCluster option (does not exist) or
// the e2eCluster fixture wiring --9p-port (also does not exist). Out of T14
// scope.
var _ = ginkgo.Describe("Phase 0 to 2 P9 anon close", ginkgo.Label("p9", "phase-transition"), func() {
	describePhase0to2P9Context("SingleNode", func(tb testing.TB) *p9Target {
		return newSingleNodeP9Target(tb)
	})
})

func describePhase0to2P9Context(name string, factory func(testing.TB) *p9Target) {
	ginkgo.Context(name, func() {
		// Each spec needs a fresh Phase 0 fixture because the first-SA-create
		// flip is one-way. Use BeforeEach (not BeforeAll) — Phase 0 is consumed
		// by the flip-to-2 path inside the spec.
		var tgt *p9Target

		ginkgo.BeforeEach(func() {
			tgt = factory(ginkgo.GinkgoTB())
		})

		// AnonMountActive_FlipPhase2_NextOpRejected: pre-flip anon attach to
		// /default works; flip Phase 2; post-flip anon attach to a non-default
		// bucket is denied.
		ginkgo.It("rejects post-flip anon attach to non-default bucket (AnonMountActive_FlipPhase2_NextOpRejected)", func() {
			t := ginkgo.GinkgoTB()
			require.True(t, isAnonEnabled(t, tgt.adminSock(0)),
				"single-node fixture must start in Phase 0 for this case")

			// Step 1: anon attach to /default.
			root, cli, err := attachP9(t, tgt, 0, "default")
			require.NoError(t, err, "anon attach to /default must succeed pre-flip")
			defer cli.Close()
			defer closeP9File(root)

			// Step 2: first op succeeds.
			_, _, _, err = root.GetAttr(p9.AttrMaskAll)
			require.NoError(t, err, "first GetAttr on anon-bound root must succeed")

			// Step 3: flip to Phase 2 via first SA create.
			seedTrustedProxyForFlip(t, tgt.adminSock(0))
			flipToPhase2(t, tgt.adminSock(0))
			require.Eventually(t, func() bool {
				return !isAnonEnabled(t, tgt.adminSock(0))
			}, 5*time.Second, 50*time.Millisecond,
				"iam.anon-enabled must flip to false after first SA create")

			// Step 4: a fresh anon attach to a non-default bucket must be denied
			// (Phase 2 + non-default + no policy = deny on the 9PAttach gate).
			nonDefaultBucket := "phase2-private-" + sanitizeForBucket(tgt.name)
			require.NoError(t,
				adminCreateBucket(t, tgt.adminSock(0), nonDefaultBucket),
				"create non-default bucket via admin UDS")

			f2, cli2, err := attachP9(t, tgt, 0, nonDefaultBucket)
			if f2 != nil {
				closeP9File(f2)
			}
			if cli2 != nil {
				defer cli2.Close()
			}
			gomega.Expect(err).To(gomega.HaveOccurred(),
				"anon attach to non-default bucket after Phase 2 flip must be denied (target=%s)",
				tgt.name)
			gomega.Expect(isEACCES(err) || isENOENT(err)).To(gomega.BeTrue(),
				"expected EACCES or ENOENT after flip, got: %v", err)
		})
	})
}
