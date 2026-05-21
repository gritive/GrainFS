package e2e

import (
	"context"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// S3SSE exercises the SSE-S3 (AES256) S3 surface (PUT/GET/HEAD/COPY)
// plus the not-implemented branches for SSE-KMS and SSE-C. Shared single +
// shared cluster fixtures, uniqueBucket per sub-test.
var _ = ginkgo.Describe("S3 SSE", ginkgo.Label("s3"), func() {
	describeS3SSEContext("SingleNode", func() s3Target {
		return newSingleNodeS3Target()
	})
	describeS3SSEContext("Cluster4Node", func() s3Target {
		return newSharedClusterS3Target(ginkgo.GinkgoTB())
	})
})

func describeS3SSEContext(name string, factory func() s3Target) {
	ginkgo.Context(name, func() {
		var (
			tgt s3Target
			cli *s3.Client
		)

		ginkgo.BeforeEach(func() {
			tgt = factory()
			cli = tgt.pickNode(0)
		})

		runS3SSECases(func() s3Target { return tgt }, func() *s3.Client { return cli })
	})
}

func runS3SSECases(getTgt func() s3Target, getClient func() *s3.Client) {
	ginkgo.It("round-trips AES256 metadata through PUT, HEAD, and GET", func(ctx context.Context) {
		bucket := createSpecBucket(getTgt(), "sseaes")
		cli := getClient()

		putOut := putSSEAESObjectEventually(ctx, getTgt(), bucket, "src.txt", "sse data")
		gomega.Expect(putOut.ServerSideEncryption).To(gomega.Equal(types.ServerSideEncryptionAes256))

		headOut, err := cli.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("src.txt"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(headOut.ServerSideEncryption).To(gomega.Equal(types.ServerSideEncryptionAes256))

		getOut, err := cli.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("src.txt"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		_, _ = io.ReadAll(getOut.Body)
		gomega.Expect(getOut.Body.Close()).To(gomega.Succeed())
		gomega.Expect(getOut.ServerSideEncryption).To(gomega.Equal(types.ServerSideEncryptionAes256))
	}, ginkgo.NodeTimeout(60*time.Second))

	ginkgo.It("preserves AES256 metadata when copying objects", func(ctx context.Context) {
		bucket := createSpecBucket(getTgt(), "ssecopy")
		cli := getClient()

		_ = putSSEAESObjectEventually(ctx, getTgt(), bucket, "src.txt", "sse data")

		copyOut, err := cli.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String("copy.txt"),
			CopySource: aws.String(bucket + "/src.txt"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(copyOut.ServerSideEncryption).To(gomega.Equal(types.ServerSideEncryptionAes256))

		copyHeadOut, err := cli.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("copy.txt"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(copyHeadOut.ServerSideEncryption).To(gomega.Equal(types.ServerSideEncryptionAes256))
	}, ginkgo.NodeTimeout(60*time.Second))

	ginkgo.It("rejects SSE-KMS as not implemented", func(ctx context.Context) {
		bucket := createSpecBucket(getTgt(), "ssekms")
		cli := getClient()

		_, err := cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket:               aws.String(bucket),
			Key:                  aws.String("kms.txt"),
			Body:                 strings.NewReader("kms"),
			ServerSideEncryption: types.ServerSideEncryptionAwsKms,
			SSEKMSKeyId:          aws.String("key-1"),
		})
		requireS3ErrorCode(err, "NotImplemented")
	}, ginkgo.NodeTimeout(60*time.Second))

	ginkgo.It("rejects SSE-C as not implemented", func(ctx context.Context) {
		bucket := createSpecBucket(getTgt(), "ssec")
		cli := getClient()

		_, err := cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket:               aws.String(bucket),
			Key:                  aws.String("sse-c.txt"),
			Body:                 strings.NewReader("sse-c"),
			SSECustomerAlgorithm: aws.String("AES256"),
			SSECustomerKey:       aws.String("customer-key"),
		})
		requireS3ErrorCode(err, "NotImplemented")
	}, ginkgo.NodeTimeout(60*time.Second))
}

func requireS3ErrorCode(err error, code string) {
	ginkgo.GinkgoHelper()
	gomega.Expect(err).To(gomega.HaveOccurred())
	var apiErr smithy.APIError
	gomega.Expect(errors.As(err, &apiErr)).To(gomega.BeTrue())
	gomega.Expect(apiErr.ErrorCode()).To(gomega.Equal(code))
}

func putSSEAESObjectEventually(ctx context.Context, tgt s3Target, bucket, key, body string) *s3.PutObjectOutput {
	ginkgo.GinkgoHelper()
	var out *s3.PutObjectOutput
	gomega.Eventually(func() error {
		var err error
		for i := 0; i < tgt.nodes; i++ {
			out, err = tgt.pickNode(i).PutObject(ctx, &s3.PutObjectInput{
				Bucket:               aws.String(bucket),
				Key:                  aws.String(key),
				Body:                 strings.NewReader(body),
				ServerSideEncryption: types.ServerSideEncryptionAes256,
			})
			if err == nil {
				return nil
			}
		}
		return err
	}, 30*time.Second, 500*time.Millisecond).Should(gomega.Succeed())
	return out
}
