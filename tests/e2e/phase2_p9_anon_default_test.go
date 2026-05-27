package e2e

import (
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// Phase 2 P9 anon attach on default bucket exercises FU#6 (F-§B-9P-anon-attach-phase2).
//
// Invariant: the "default" bucket carries an implicit-anon Allow via §9 F#41b
// (s3auth.ReasonDefaultBucketImplicitAnon, the D#2 carve-out in
// internal/s3auth/authorizer.go). The carve-out is action-agnostic and
// Phase-agnostic — anon 9P attach to /default must succeed after bootstrap,
// matching the S3 anon GET /default/* parity (authn_middleware.go
// DefaultBucketName branch).
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

		ginkgo.It("allows anon attach to /default after first SA create (DefaultBucketAnonAttachOK)", func() {
			t := ginkgo.GinkgoTB()

			flipToPhase2(t, tgt.adminSock(0))

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
