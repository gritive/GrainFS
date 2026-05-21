package e2e

import (
	"context"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// IAM MountSA validates the MountSA admin plane (create/list/get/delete,
// policy attach/detach, cross-namespace guard) against both single-node and
// cluster fixtures.
var _ = ginkgo.Describe("IAM MountSA", ginkgo.Label("iam", "mountsa"), func() {
	describeIAMMountSAContext("SingleNode", func(testing.TB) iamAdminTarget {
		return newSingleNodeIAMAdminTarget()
	})
	describeIAMMountSAContext("Cluster4Node", func(tb testing.TB) iamAdminTarget {
		return newSharedClusterIAMAdminTarget(tb)
	})
})

func describeIAMMountSAContext(name string, factory func(testing.TB) iamAdminTarget) {
	ginkgo.Context(name, func() {
		var (
			ctx context.Context
			tgt iamAdminTarget
		)

		ginkgo.BeforeEach(func() {
			ctx = context.Background()
			tgt = factory(ginkgo.GinkgoTB())
		})

		runIAMMountSACases(func() context.Context { return ctx }, func() iamAdminTarget { return tgt })
	})
}

// mountSANameFor produces a short, unique MountSA name for the given target + case.
func mountSANameFor(tgtName, caseName string) string {
	return "e2e-msa-" + sanitizeForBucket(tgtName) + "-" + sanitizeForBucket(caseName)
}

// runIAMMountSACases exercises MountSA CRUD, policy attach/detach, and
// cross-namespace guard against the given target.
func runIAMMountSACases(getCtx func() context.Context, getTgt func() iamAdminTarget) {
	// CreateListGetDelete: full CRUD round-trip.
	ginkgo.It("supports MountSA CRUD round-trip (CreateListGetDelete)", func() {
		ctx := getCtx()
		tgt := getTgt()
		c := tgt.iamClient()
		name := mountSANameFor(tgt.name, "crud")
		ginkgo.DeferCleanup(func() { _ = c.MountSADelete(ctx, name) })

		created, err := c.MountSACreate(ctx, name, 1001, "e2e-test")
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		gomega.Expect(created.Name).To(gomega.Equal(name))
		gomega.Expect(created.UID).To(gomega.Equal(uint32(1001)))

		// List must contain the new entry.
		items, err := c.MountSAList(ctx)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		found := false
		for _, it := range items {
			if it.Name == name {
				found = true
				gomega.Expect(it.UID).To(gomega.Equal(uint32(1001)))
			}
		}
		gomega.Expect(found).To(gomega.BeTrue(), "mount-sa %q must appear in list", name)

		// Get must return the entry.
		got, err := c.MountSAGet(ctx, name)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		gomega.Expect(got.Name).To(gomega.Equal(name))
		gomega.Expect(got.UID).To(gomega.Equal(uint32(1001)))

		// Delete must succeed.
		gomega.Expect(c.MountSADelete(ctx, name)).ToNot(gomega.HaveOccurred())

		// Get on deleted entry must return 404.
		_, err = c.MountSAGet(ctx, name)
		gomega.Expect(err).To(gomega.HaveOccurred(), "Get on deleted mount-sa must error")
	})

	// PolicyAttachDetach: attach the built-in NFSMountOnly, then detach it.
	ginkgo.It("attaches and detaches NFSMountOnly (PolicyAttachDetach)", func() {
		ctx := getCtx()
		tgt := getTgt()
		c := tgt.iamClient()
		name := mountSANameFor(tgt.name, "policy-attach")
		ginkgo.DeferCleanup(func() { _ = c.MountSADelete(ctx, name) })

		_, err := c.MountSACreate(ctx, name, 1002, "")
		gomega.Expect(err).ToNot(gomega.HaveOccurred())

		gomega.Expect(c.MountSAPolicyAttach(ctx, name, "NFSMountOnly")).ToNot(gomega.HaveOccurred())
		gomega.Expect(c.MountSAPolicyDetach(ctx, name, "NFSMountOnly")).ToNot(gomega.HaveOccurred())
	})

	// CrossNamespaceGuard: attaching an S3-SA builtin to a MountSA must be
	// rejected (403 Forbidden via ValidateForMountSAAttach).
	ginkgo.It("rejects attaching S3 policy to MountSA (CrossNamespaceGuard_RejectS3Policy)", func() {
		ctx := getCtx()
		tgt := getTgt()
		c := tgt.iamClient()
		name := mountSANameFor(tgt.name, "xns-guard")
		ginkgo.DeferCleanup(func() { _ = c.MountSADelete(ctx, name) })

		_, err := c.MountSACreate(ctx, name, 1003, "")
		gomega.Expect(err).ToNot(gomega.HaveOccurred())

		// "readonly" is an S3/Iceberg policy — must be rejected for MountSA attach.
		err = c.MountSAPolicyAttach(ctx, name, "readonly")
		gomega.Expect(err).To(gomega.HaveOccurred(), "attaching S3 policy to MountSA must be rejected")
	})

	// GetNotFound: GET on a non-existent MountSA must return a non-nil error.
	ginkgo.It("returns error on Get for missing MountSA (GetNotFound)", func() {
		ctx := getCtx()
		tgt := getTgt()
		c := tgt.iamClient()
		_, err := c.MountSAGet(ctx, "does-not-exist-"+sanitizeForBucket(tgt.name))
		gomega.Expect(err).To(gomega.HaveOccurred(), "GET on missing mount-sa must error")
	})
}
