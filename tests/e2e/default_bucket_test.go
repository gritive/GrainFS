package e2e

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Default bucket", ginkgo.Label("bucket"), func() {
	ginkgo.It("exists on single-node startup", func() {
		ctx := context.Background()

		// The "default" bucket should exist immediately after server startup
		out, err := testS3Client.ListBuckets(ctx, &s3.ListBucketsInput{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		found := false
		for _, b := range out.Buckets {
			if aws.ToString(b.Name) == "default" {
				found = true
				break
			}
		}
		gomega.Expect(found).To(gomega.BeTrue())

		// Verify we can use the default bucket immediately.
		_, err = testS3Client.HeadBucket(ctx, &s3.HeadBucketInput{
			Bucket: aws.String("default"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	ginkgo.It("exists on cluster startup", func() {
		_ = newSharedClusterS3Target(ginkgo.GinkgoTB())
	})
})
