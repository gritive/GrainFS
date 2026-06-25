package e2e

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// S3GetBucketLocation exercises the GetBucketLocation operation: the happy path
// returns GrainFS's literal "us-east-1" LocationConstraint, and a missing bucket
// surfaces a NoSuchBucket API error. (Note: aws-sdk-go-v2 v1.104.0 has no named
// us-east-1 enum constant — AWS treats us-east-1 as the null default — so the
// happy assertion compares against the string-cast BucketLocationConstraint.)
var _ = ginkgo.Describe("S3 GetBucketLocation", ginkgo.Label("s3"), func() {
	describeS3BucketLocationContext("SingleNode", func() s3Target {
		return newSingleNodeS3Target()
	})
	describeS3BucketLocationContext("Cluster4Node", func() s3Target {
		return newSharedClusterS3Target(ginkgo.GinkgoTB())
	})
})

func describeS3BucketLocationContext(name string, factory func() s3Target) {
	ginkgo.Context(name, ginkgo.Ordered, func() {
		var (
			tgt s3Target
			cli *s3.Client
		)

		ginkgo.BeforeAll(func() {
			tgt = factory()
			cli = tgt.pickNode(0)
		})

		runS3BucketLocationCases(func() s3Target { return tgt }, func() *s3.Client { return cli })
	})
}

func runS3BucketLocationCases(getTgt func() s3Target, getClient func() *s3.Client) {
	ginkgo.It("returns the us-east-1 LocationConstraint for an existing bucket", func(ctx context.Context) {
		bucket := createSpecBucket(getTgt(), "loc")
		cli := getClient()

		out, err := cli.GetBucketLocation(ctx, &s3.GetBucketLocationInput{
			Bucket: aws.String(bucket),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(out.LocationConstraint).To(gomega.Equal(types.BucketLocationConstraint("us-east-1")))
	}, ginkgo.NodeTimeout(60*time.Second))

	ginkgo.It("returns NoSuchBucket for a missing bucket", func(ctx context.Context) {
		cli := getClient()
		// Synthesize a name that is never created so the lookup misses.
		missing := getTgt().name + "-getbucketloc-missing"

		_, err := cli.GetBucketLocation(ctx, &s3.GetBucketLocationInput{
			Bucket: aws.String(missing),
		})
		requireS3ErrorCode(err, "NoSuchBucket")
	}, ginkgo.NodeTimeout(60*time.Second))
}
