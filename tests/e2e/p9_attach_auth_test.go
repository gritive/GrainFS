package e2e

import (
	"context"
	"strings"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"github.com/gritive/GrainFS/internal/adminapi"
	"github.com/gritive/GrainFS/internal/iamadmin"
)

// P9 attach auth exercises the wire-level 9P attach/auth gate
// (NFS§B T9 + T12 spec D#6) using the hugelgupf/p9 client over TCP.
//
// Cases (per Context viability noted inline):
//   - AnameAnon_PublicBucket_OK: anon attach to "default". SingleNode only —
//     mrCluster fixture does not auto-seed "default" (cluster bootstrap gates
//     ShouldCreateDefaultBucketOnStartup on len(peers)==0; admin UDS refuses
//     reserved names via public API), so cluster cannot run this case without
//     extra fixture plumbing. FU#6 (F-§B-9P-anon-attach-phase2) fixed the
//     underlying gap: bucketFile/objectFile.anonRejected now carves out the
//     "default" bucket so the per-op flip gate honors the D#2 implicit-anon
//     promise from rootFile.resolveAnon — see tests/e2e/phase2_p9_anon_default_test.go.
//   - AnameMountSAHit_OK: enabled — FU#5 (F-§B-resolver-mountsa) wires
//     resolver to consult the mount-SA pool when RequestContext.PrincipalType
//     is PrincipalTypeMount, which 9P attach now sets via p9AttachReqCtx.
//   - AnameMountSAMiss_ENOENT: typo not in pool → ENOENT. Works on both
//     SingleNode and Cluster3Node (resolver pre-check, phase-agnostic).
//   - AnameMountSANoPolicy_EACCES: mount-SA in pool but no policy attached →
//     EACCES. Works on both (deny path, phase-agnostic).
var _ = ginkgo.Describe("P9 attach auth", ginkgo.Label("p9", "auth"), func() {
	describeP9AttachAuthContext("SingleNode", false, func(tb testing.TB) *p9Target {
		return newSingleNodeP9Target(tb)
	})
	describeP9AttachAuthContext("Cluster3Node", true, func(tb testing.TB) *p9Target {
		return newClusterP9Target(tb)
	})
})

// describeP9AttachAuthContext wires the case set. AnonPublicBucket only runs
// against SingleNode (cluster is always Phase 2 — see top-level comment).
func describeP9AttachAuthContext(name string, isCluster bool, factory func(testing.TB) *p9Target) {
	ginkgo.Context(name, ginkgo.Ordered, func() {
		var tgt *p9Target

		ginkgo.BeforeAll(func() {
			tgt = factory(ginkgo.GinkgoTB())
		})

		if !isCluster {
			// SingleNode-only: Phase 0 anon attach to /default.
			ginkgo.It("allows anon attach to /default (AnameAnon_PublicBucket_OK)", func() {
				t := ginkgo.GinkgoTB()
				f, cli, err := attachP9(t, tgt, 0, "default")
				gomega.Expect(err).NotTo(gomega.HaveOccurred(),
					"anon attach to /default must succeed (target=%s)", tgt.name)
				closeP9File(f)
				_ = cli.Close()
			})
		}

		ginkgo.Context("with bootstrapped MountSAs", ginkgo.Ordered, func() {
			var (
				ctx         context.Context
				adminClient *iamadmin.Client
				mountSAName string
				typoName    string
				bobName     string
			)

			ginkgo.BeforeAll(func() {
				t := ginkgo.GinkgoTB()
				ctx = context.Background()
				ensureBootstrapped(t, tgt)
				adminClient = iamadminClientForSock(tgt.adminSock(0))

				mountSAName = "alice-mount-" + sanitizeForBucket(tgt.name)
				typoName = "typo-mount-" + sanitizeForBucket(tgt.name)
				bobName = "bob-mount-" + sanitizeForBucket(tgt.name)

				_, err := adminClient.MountSACreate(ctx, mountSAName, 200001, "p9-e2e-alice")
				gomega.Expect(err).ToNot(gomega.HaveOccurred(), "create alice mount-sa")
				gomega.Expect(
					adminClient.MountSAPolicyAttach(ctx, mountSAName, "9PAttachOnly"),
				).ToNot(gomega.HaveOccurred(), "attach 9PAttachOnly to alice mount-sa")

				_, err = adminClient.MountSACreate(ctx, bobName, 200002, "p9-e2e-bob")
				gomega.Expect(err).ToNot(gomega.HaveOccurred(), "create bob mount-sa (no policy)")
			})

			ginkgo.AfterAll(func() {
				if adminClient == nil {
					return
				}
				_ = adminClient.MountSAPolicyDetach(ctx, mountSAName, "9PAttachOnly")
				_ = adminClient.MountSADelete(ctx, mountSAName)
				_ = adminClient.MountSADelete(ctx, bobName)
			})

			// AnameMountSAHit_OK: alice in pool + 9PAttachOnly attached.
			// FU#5 (F-§B-resolver-mountsa) wired the resolver to the
			// mount-SA pool, so this now passes on both SingleNode and
			// Cluster3Node.
			ginkgo.It("hits MountSA with policy (AnameMountSAHit_OK)", func() {
				t := ginkgo.GinkgoTB()
				aname := mountSAName + "@default"
				f, cli, err := attachP9(t, tgt, 0, aname)
				gomega.Expect(err).ToNot(gomega.HaveOccurred(),
					"mount-sa attach with 9PAttachOnly must succeed (aname=%s target=%s)",
					aname, tgt.name)
				if f != nil {
					closeP9File(f)
				}
				if cli != nil {
					_ = cli.Close()
				}
			})

			// AnameMountSAMiss_ENOENT: typo not in pool → ENOENT.
			ginkgo.It("misses MountSA with ENOENT (AnameMountSAMiss_ENOENT)", func() {
				t := ginkgo.GinkgoTB()
				f, cli, err := attachP9(t, tgt, 0, typoName+"@default")
				if f != nil {
					closeP9File(f)
				}
				if cli != nil {
					defer cli.Close()
				}
				gomega.Expect(err).To(gomega.HaveOccurred(),
					"mount-sa miss attach must fail (aname=%s@default target=%s)",
					typoName, tgt.name)
				gomega.Expect(isENOENT(err)).To(gomega.BeTrue(),
					"mount-sa miss must surface ENOENT, got: %v", err)
			})

			// AnameMountSANoPolicy_EACCES: bob in pool but no policy → EACCES.
			ginkgo.It("denies MountSA without policy (AnameMountSANoPolicy_EACCES)", func() {
				t := ginkgo.GinkgoTB()
				f, cli, err := attachP9(t, tgt, 0, bobName+"@default")
				if f != nil {
					closeP9File(f)
				}
				if cli != nil {
					defer cli.Close()
				}
				gomega.Expect(err).To(gomega.HaveOccurred(),
					"mount-sa attach without policy must fail (aname=%s@default target=%s)",
					bobName, tgt.name)
				gomega.Expect(isEACCES(err)).To(gomega.BeTrue(),
					"mount-sa no-policy must surface EACCES, got: %v", err)
			})
		})
	})
}

// iamadminClientForSock builds an iamadmin.Client wired to an admin UDS path.
func iamadminClientForSock(sock string) *iamadmin.Client {
	tp, _ := adminapi.NewTransport(sock)
	return &iamadmin.Client{Transport: tp}
}

// isENOENT inspects the error chain for syscall.ENOENT or hugelgupf/p9's
// ENOENT mapping (string "no such file or directory" appears in the wrapped
// linux.Errno).
func isENOENT(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "no such file or directory") ||
		strings.Contains(msg, "ENOENT")
}

// isEACCES inspects the error chain for syscall.EACCES or hugelgupf/p9's
// EACCES mapping (string "permission denied").
func isEACCES(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "permission denied") ||
		strings.Contains(msg, "EACCES")
}
