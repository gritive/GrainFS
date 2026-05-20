// Bucket-level S3 API e2e (target table-driven).
//
// The same case set runs against a single-node fixture and a 4-node cluster
// fixture. Bucket names are derived from t.Name()+case via tgt.uniqueBucket
// to avoid namespace collisions in the shared cluster fixture.
package e2e

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

func TestBucketsE2E(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "Buckets e2e")
}

var _ = ginkgo.Describe("Buckets", func() {
	describeBucketContext("SingleNode", func() s3Target {
		return newSingleNodeS3Target()
	})
	describeBucketContext("Cluster4Node", func() s3Target {
		return newSharedClusterS3Target(ginkgo.GinkgoTB())
	})
})

func describeBucketContext(name string, factory func() s3Target) {
	ginkgo.Context(name, func() {
		var (
			ctx    context.Context
			tgt    s3Target
			client *s3.Client
		)

		ginkgo.BeforeEach(func() {
			ctx = context.Background()
			tgt = factory()
			client = tgt.pickNode(0)
		})

		runBucketCases(func() context.Context { return ctx }, func() s3Target { return tgt }, func() *s3.Client { return client })
	})
}

func runBucketCases(getCtx func() context.Context, getTgt func() s3Target, getClient func() *s3.Client) {
	ginkgo.It("creates buckets through the admin fixture (Create)", func() {
		tgt := getTgt()
		// uniqueBucket exercises CreateBucket internally and registers
		// auto-cleanup; reaching this line means creation succeeded.
		_ = createSpecBucket(tgt, "create")
	})

	ginkgo.It("heads an existing bucket (Head)", func() {
		ctx := getCtx()
		client := getClient()
		name := createSpecBucket(getTgt(), "head")

		_, err := client.HeadBucket(ctx, &s3.HeadBucketInput{
			Bucket: aws.String(name),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	ginkgo.It("returns NotFound when heading a missing bucket (HeadNotFound)", func() {
		ctx := getCtx()
		tgt := getTgt()
		client := getClient()
		// Synthesize a name that won't collide with any uniqueBucket.
		name := tgt.name + "-headnotfound-missing"
		_, err := client.HeadBucket(ctx, &s3.HeadBucketInput{
			Bucket: aws.String(name),
		})
		gomega.Expect(err).To(gomega.HaveOccurred())

		var apiErr smithy.APIError
		gomega.Expect(errors.As(err, &apiErr)).To(gomega.BeTrue())
		gomega.Expect(apiErr.ErrorCode()).To(gomega.Equal("NotFound"))
	})

	ginkgo.It("denies data-plane bucket creation (CreateDeniedOnDataPlane)", func() {
		ctx := getCtx()
		client := getClient()
		name := createSpecBucket(getTgt(), "conflict")

		_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String(name),
		})
		gomega.Expect(err).To(gomega.HaveOccurred())

		var apiErr smithy.APIError
		gomega.Expect(errors.As(err, &apiErr)).To(gomega.BeTrue())
		gomega.Expect(apiErr.ErrorCode()).To(gomega.Equal("AccessDenied"))
	})

	ginkgo.It("lists created buckets (List)", func() {
		ctx := getCtx()
		client := getClient()
		name := createSpecBucket(getTgt(), "list")

		out, err := client.ListBuckets(ctx, &s3.ListBucketsInput{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		found := false
		for _, b := range out.Buckets {
			if aws.ToString(b.Name) == name {
				found = true
				break
			}
		}
		gomega.Expect(found).To(gomega.BeTrue(), "bucket %q should appear in ListBuckets", name)
	})

	ginkgo.It("denies data-plane bucket deletion (Delete)", func() {
		ctx := getCtx()
		client := getClient()
		name := createSpecBucket(getTgt(), "delete")

		_, err := client.DeleteBucket(ctx, &s3.DeleteBucketInput{
			Bucket: aws.String(name),
		})
		gomega.Expect(err).To(gomega.HaveOccurred())

		var apiErr smithy.APIError
		gomega.Expect(errors.As(err, &apiErr)).To(gomega.BeTrue())
		gomega.Expect(apiErr.ErrorCode()).To(gomega.Equal("AccessDenied"))
	})

	ginkgo.It("denies data-plane deletion for non-empty buckets (DeleteNotEmpty)", func() {
		ctx := getCtx()
		client := getClient()
		name := createSpecBucket(getTgt(), "notempty")

		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(name),
			Key:    aws.String("file.txt"),
			Body:   stringReader("data"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		_, err = client.DeleteBucket(ctx, &s3.DeleteBucketInput{
			Bucket: aws.String(name),
		})
		gomega.Expect(err).To(gomega.HaveOccurred())

		var apiErr smithy.APIError
		gomega.Expect(errors.As(err, &apiErr)).To(gomega.BeTrue())
		gomega.Expect(apiErr.ErrorCode()).To(gomega.Equal("AccessDenied"))
	})

	ginkgo.It("denies internal bucket creation and hides internal buckets from lists (InternalBucketDeniedAndNotListed)", func() {
		ctx := getCtx()
		tgt := getTgt()
		client := getClient()

		// Data-plane bucket lifecycle is admin-UDS-only; internal buckets are
		// also rejected before storage creation.
		internalName := "__grainfs_review_test_internal_" + tgt.name
		_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(internalName)})
		gomega.Expect(err).To(gomega.HaveOccurred())

		out, err := client.ListBuckets(ctx, &s3.ListBucketsInput{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for _, b := range out.Buckets {
			name := aws.ToString(b.Name)
			if strings.HasPrefix(name, "__grainfs_") {
				ginkgo.Fail("ListBuckets exposed internal bucket " + name)
			}
		}
	})
}

func createSpecBucket(tgt s3Target, caseName string) string {
	tb := ginkgo.GinkgoTB()
	name := bucketNameFor(tgt.name, ginkgo.CurrentSpecReport().FullText(), caseName)
	tgt.createBkt(tb, name)
	ginkgo.DeferCleanup(func(ctx context.Context) {
		_, _ = tgt.pickNode(0).DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(name)})
	}, ginkgo.NodeTimeout(10*time.Second))
	return name
}
