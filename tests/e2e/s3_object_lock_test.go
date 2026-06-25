package e2e

import (
	"context"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// S3ObjectLock exercises the Object Lock surface (retention / legal-hold /
// lock-configuration / object-lock PutObject headers). None of it is
// implemented; every operation must be rejected fail-closed with 501
// NotImplemented rather than the prior fail-open behavior (false 200, mis-
// delivered object bytes, or — for legal-hold PUT — silent object overwrite).
var _ = ginkgo.Describe("S3 Object Lock", ginkgo.Label("s3"), func() {
	describeS3ObjectLockContext("SingleNode", func() s3Target {
		return newSingleNodeS3Target()
	})
	describeS3ObjectLockContext("Cluster4Node", func() s3Target {
		return newSharedClusterS3Target(ginkgo.GinkgoTB())
	})
})

func describeS3ObjectLockContext(name string, factory func() s3Target) {
	ginkgo.Context(name, ginkgo.Ordered, func() {
		var (
			tgt s3Target
			cli *s3.Client
		)

		ginkgo.BeforeAll(func() {
			tgt = factory()
			cli = tgt.pickNode(0)
		})

		runS3ObjectLockCases(func() s3Target { return tgt }, func() *s3.Client { return cli })
	})
}

func runS3ObjectLockCases(getTgt func() s3Target, getClient func() *s3.Client) {
	ginkgo.It("rejects PutObjectRetention as not implemented", func(ctx context.Context) {
		bucket := createSpecBucket(getTgt(), "lockret")
		cli := getClient()
		putPlainObjectEventually(ctx, getTgt(), bucket, "obj.txt", "body")

		_, err := cli.PutObjectRetention(ctx, &s3.PutObjectRetentionInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("obj.txt"),
			Retention: &types.ObjectLockRetention{
				Mode:            types.ObjectLockRetentionModeGovernance,
				RetainUntilDate: aws.Time(time.Now().Add(48 * time.Hour)),
			},
		})
		requireS3ErrorCode(err, "NotImplemented")
	}, ginkgo.NodeTimeout(60*time.Second))

	ginkgo.It("rejects GetObjectRetention as not implemented", func(ctx context.Context) {
		bucket := createSpecBucket(getTgt(), "lockgetret")
		cli := getClient()
		putPlainObjectEventually(ctx, getTgt(), bucket, "obj.txt", "body")

		_, err := cli.GetObjectRetention(ctx, &s3.GetObjectRetentionInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("obj.txt"),
		})
		requireS3ErrorCode(err, "NotImplemented")
	}, ginkgo.NodeTimeout(60*time.Second))

	ginkgo.It("rejects GetObjectLockConfiguration as not implemented", func(ctx context.Context) {
		bucket := createSpecBucket(getTgt(), "lockcfg")
		cli := getClient()

		_, err := cli.GetObjectLockConfiguration(ctx, &s3.GetObjectLockConfigurationInput{
			Bucket: aws.String(bucket),
		})
		requireS3ErrorCode(err, "NotImplemented")
	}, ginkgo.NodeTimeout(60*time.Second))

	ginkgo.It("rejects GetObjectLegalHold as not implemented", func(ctx context.Context) {
		bucket := createSpecBucket(getTgt(), "lockgetlh")
		cli := getClient()
		putPlainObjectEventually(ctx, getTgt(), bucket, "obj.txt", "body")

		_, err := cli.GetObjectLegalHold(ctx, &s3.GetObjectLegalHoldInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("obj.txt"),
		})
		requireS3ErrorCode(err, "NotImplemented")
	}, ginkgo.NodeTimeout(60*time.Second))

	ginkgo.It("rejects PutObjectLegalHold without corrupting the object", func(ctx context.Context) {
		bucket := createSpecBucket(getTgt(), "lockputlh")
		cli := getClient()
		const original = "original-bytes"
		putPlainObjectEventually(ctx, getTgt(), bucket, "obj.txt", original)

		_, err := cli.PutObjectLegalHold(ctx, &s3.PutObjectLegalHoldInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("obj.txt"),
			LegalHold: &types.ObjectLockLegalHold{
				Status: types.ObjectLockLegalHoldStatusOn,
			},
		})
		requireS3ErrorCode(err, "NotImplemented")

		// #4 regression guard: the rejected legal-hold PUT must NOT have
		// overwritten the object body with the legal-hold XML.
		getOut, getErr := cli.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("obj.txt"),
		})
		gomega.Expect(getErr).NotTo(gomega.HaveOccurred())
		got, _ := io.ReadAll(getOut.Body)
		gomega.Expect(getOut.Body.Close()).To(gomega.Succeed())
		gomega.Expect(string(got)).To(gomega.Equal(original))
	}, ginkgo.NodeTimeout(60*time.Second))

	ginkgo.It("rejects PutObject carrying Object Lock headers as not implemented", func(ctx context.Context) {
		bucket := createSpecBucket(getTgt(), "lockhdr")
		cli := getClient()

		_, err := cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket:                    aws.String(bucket),
			Key:                       aws.String("locked.txt"),
			Body:                      strings.NewReader("locked"),
			ObjectLockMode:            types.ObjectLockModeGovernance,
			ObjectLockRetainUntilDate: aws.Time(time.Now().Add(48 * time.Hour)),
			ObjectLockLegalHoldStatus: types.ObjectLockLegalHoldStatusOn,
		})
		requireS3ErrorCode(err, "NotImplemented")
	}, ginkgo.NodeTimeout(60*time.Second))
}

func putPlainObjectEventually(ctx context.Context, tgt s3Target, bucket, key, body string) {
	ginkgo.GinkgoHelper()
	gomega.Eventually(func() error {
		var err error
		for i := 0; i < tgt.nodes; i++ {
			_, err = tgt.pickNode(i).PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
				Body:   strings.NewReader(body),
			})
			if err == nil {
				return nil
			}
		}
		return err
	}, 30*time.Second, 500*time.Millisecond).Should(gomega.Succeed())
}
