package e2e

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// Phase 2 P9 anon attach on default bucket exercises FU#6 (F-§B-9P-anon-attach-phase2).
//
// Invariant: the "default" bucket carries an implicit-anon Allow via §9 F#41b
// (s3auth.ReasonDefaultBucketImplicitAnon, the D#2 carve-out in
// internal/s3auth/authorizer.go). The carve-out is action-agnostic and
// Phase-agnostic — anon 9P attach to /default must succeed even after the
// Phase 0→Phase 2 flip (iam.anon-enabled=false), matching the S3 anon GET
// /default/* parity (authn_middleware.go DefaultBucketName branch).
//
// The earlier failure mode (FU#6): rootFile.resolveAnon correctly returned
// Allow via D#2 at attach time, but hugelgupf/p9's tattach handler
// (handlers.go: handle tattach → doWalk → walkOne → from.GetAttr) called
// bucketFile.GetAttr immediately after the Walk, which routed through
// bucketFile.anonRejected(). The flip gate there returned true for any
// anon-bound binding in Phase 2 — without honoring D#2 — and returned EACCES,
// killing the attach. The fix: anonRejected() carves out f.bucket=="default".
//
// SingleNode-only. Cluster3Node would also exhibit the bug, but
// newClusterP9Target's underlying mrCluster does not auto-seed "default"
// (ShouldCreateDefaultBucketOnStartup gates on len(peers)==0, and admin UDS
// BucketCreate refuses reserved names via public API). The fix itself is
// bucket-name keyed and applies identically in cluster mode once "default"
// exists; seeding it cluster-wide is a separate concern outside FU#6.
var _ = ginkgo.Describe("Phase 2 P9 anon attach on default", ginkgo.Label("p9", "auth", "phase-transition"), func() {
	ginkgo.Context("SingleNode", func() {
		var tgt *p9Target

		ginkgo.BeforeEach(func() {
			tgt = newSingleNodeP9Target(ginkgo.GinkgoTB())
		})

		// Phase2_AnonAttachOnDefaultBucket_OK: flip Phase 0→2 via first SA
		// create, then anon attach to /default must succeed via D#2 implicit
		// anon, with no per-op flip gate retraction during attach's
		// post-Walk GetAttr.
		ginkgo.It("allows anon attach to /default after Phase 2 flip (Phase2_AnonAttachOnDefaultBucket_OK)", func() {
			t := ginkgo.GinkgoTB()
			gomega.Expect(isAnonEnabled(t, tgt.adminSock(0))).To(gomega.BeTrue(),
				"single-node fixture must start in Phase 0 for this case")

			seedTrustedProxyForFlip(t, tgt.adminSock(0))
			flipToPhase2(t, tgt.adminSock(0))
			gomega.Eventually(func() bool {
				return !isAnonEnabled(t, tgt.adminSock(0))
			}, 5*time.Second, 50*time.Millisecond).Should(gomega.BeTrue(),
				"iam.anon-enabled must flip to false after first SA create")

			f, cli, err := attachP9(t, tgt, 0, "default")
			gomega.Expect(err).ToNot(gomega.HaveOccurred(),
				"anon attach to /default in Phase 2 must succeed (D#2 implicit anon)")
			if f != nil {
				closeP9File(f)
			}
			if cli != nil {
				_ = cli.Close()
			}
		})
	})
})
