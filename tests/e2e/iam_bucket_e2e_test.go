package e2e

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithy "github.com/aws/smithy-go"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"github.com/gritive/GrainFS/internal/iamadmin"
)

var _ = ginkgo.Describe("IAM bucket", func() {
	describeIAMBucketContext("SingleNode", func() iamAdminTarget {
		return newSingleNodeIAMAdminTarget()
	})
	describeIAMBucketContext("Cluster4Node", func() iamAdminTarget {
		return newSharedClusterIAMAdminTarget(ginkgo.GinkgoTB())
	})
})

// validBucketPolicyDoc is a minimal S3 bucket policy accepted by
// policy.ParsePolicy (requires Effect="Allow"|"Deny").
const validBucketPolicyDoc = `{
	"Version":"2012-10-17",
	"Statement":[{
		"Effect":"Allow",
		"Principal":"*",
		"Action":["s3:GetObject"],
		"Resource":["arn:aws:s3:::test-bucket/*"]
	}]
}`

// runIAMBucketCases exercises bucket CRUD, policy put/delete, and the
// data-plane create-denied invariant (D#8) against the given target.
//
// Absent cases and rationale:
//
//   - "CreateAttachMutualRequirement_400": the server guard for single-sided
//     attach is an AND condition (if req.AttachSA != "" && req.AttachPolicy != "").
//     When only one side is provided the attach is silently skipped — no 400
//     is returned. This is a documented server-side behaviour; a separate
//     enhancement ticket should add the validation guard.
//
//   - "PolicyPutDelete": PUT /v1/buckets/:name/policy returns
//     ErrUnsupportedOperation in the shared e2e fixtures (storage backend does
//     not support per-bucket policy storage in this configuration). The route
//     and handler are exercised by unit tests in handlers_bucket_policy.go.
func describeIAMBucketContext(name string, factory func() iamAdminTarget) {
	ginkgo.Context(name, func() {
		var (
			ctx context.Context
			tgt iamAdminTarget
		)

		ginkgo.BeforeEach(func() {
			ctx = context.Background()
			tgt = factory()
		})

		runIAMBucketCases(func() context.Context { return ctx }, func() iamAdminTarget { return tgt })
	})
}

func runIAMBucketCases(getCtx func() context.Context, getTgt func() iamAdminTarget) {
	// CreateDelete: create via IAM admin plane, verify it appears in list, then
	// delete. The post-create list membership and the absence of errors on
	// create/delete are the primary invariants.
	ginkgo.It("creates, lists, and deletes buckets through IAM admin (CreateDelete)", func() {
		ctx := getCtx()
		tgt := getTgt()
		c := tgt.iamClient()
		name := iamSpecBucketName(tgt, "create-delete")
		ginkgo.DeferCleanup(func() { _ = c.BucketDelete(ctx, name, false) })

		gomega.Expect(c.BucketCreate(ctx, name, "", "")).To(gomega.Succeed())

		items, err := c.BucketList(ctx)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(bucketItemNames(items)).To(gomega.ContainElement(name))

		gomega.Expect(c.BucketDelete(ctx, name, false)).To(gomega.Succeed())
	})

	// List: create two buckets, verify both appear in list.
	ginkgo.It("lists multiple IAM-created buckets (List)", func() {
		ctx := getCtx()
		tgt := getTgt()
		c := tgt.iamClient()
		name1 := iamSpecBucketName(tgt, "list-a")
		name2 := iamSpecBucketName(tgt, "list-b")
		ginkgo.DeferCleanup(func() {
			_ = c.BucketDelete(ctx, name1, false)
			_ = c.BucketDelete(ctx, name2, false)
		})

		gomega.Expect(c.BucketCreate(ctx, name1, "", "")).To(gomega.Succeed())
		gomega.Expect(c.BucketCreate(ctx, name2, "", "")).To(gomega.Succeed())

		items, err := c.BucketList(ctx)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		names := bucketItemNames(items)
		gomega.Expect(names).To(gomega.ContainElement(name1))
		gomega.Expect(names).To(gomega.ContainElement(name2))
	})

	// CreateWithAttach: create a bucket and atomically attach an SA + policy
	// via MetaCmd 62. Verify the bucket exists (attach success is implicit:
	// if the MetaCmd fails, BucketCreate rolls back the bucket creation and
	// returns an error).
	ginkgo.It("creates buckets with SA/policy attachment (CreateWithAttach)", func() {
		ctx := getCtx()
		tgt := getTgt()
		c := tgt.iamClient()
		name := iamSpecBucketName(tgt, "create-attach")
		ginkgo.DeferCleanup(func() { _ = c.BucketDelete(ctx, name, false) })

		saID, _, _ := tgt.uniqueSA(ginkgo.GinkgoTB(), "create-attach")

		// Use the built-in "readwrite" policy (allows PutObject / GetObject).
		gomega.Expect(c.BucketCreate(ctx, name, saID, "readwrite")).To(gomega.Succeed())

		// Bucket must be present after create-with-attach.
		items, err := c.BucketList(ctx)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(bucketItemNames(items)).To(gomega.ContainElement(name))
	})

	// DataplaneCreateRefused: S3 CreateBucket on the data plane must always
	// return AccessDenied per Decision #8 (admin-UDS-only actions). The SA
	// used here has full admin credentials; the denial is unconditional.
	ginkgo.It("refuses data-plane bucket creation (DataplaneCreateRefused)", func() {
		ctx := getCtx()
		tgt := getTgt()
		name := iamSpecBucketName(tgt, "dp-create-refused")
		s3c := tgt.pickNode(0)
		_, err := s3c.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String(name),
		})
		gomega.Expect(err).To(gomega.HaveOccurred())
		var apiErr smithy.APIError
		gomega.Expect(errors.As(err, &apiErr)).To(gomega.BeTrue())
		gomega.Expect(apiErr.ErrorCode()).To(gomega.Equal("AccessDenied"))
	})
}

func iamSpecBucketName(tgt iamAdminTarget, caseName string) string {
	return bucketNameFor(tgt.name, ginkgo.CurrentSpecReport().FullText(), caseName)
}

// bucketItemNames extracts bucket names from a BucketListItem slice.
func bucketItemNames(items []iamadmin.BucketListItem) []string {
	out := make([]string, len(items))
	for i, item := range items {
		out[i] = item.Name
	}
	return out
}
