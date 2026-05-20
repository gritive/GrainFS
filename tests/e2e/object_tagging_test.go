//go:build integration

package e2e

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

func TestObjectTaggingE2E(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "Object tagging e2e")
}

var _ = ginkgo.Describe("Object tagging", func() {
	describeObjectTaggingContext("SingleNode", func() s3Target {
		return newSingleNodeS3Target()
	})
	describeObjectTaggingContext("Cluster4Node", func() s3Target {
		return newSharedClusterS3Target(ginkgo.GinkgoTB())
	})
})

func describeObjectTaggingContext(name string, factory func() s3Target) {
	ginkgo.Context(name, ginkgo.Ordered, func() {
		var tgt s3Target
		ginkgo.BeforeAll(func() {
			tgt = factory()
			tb := ginkgo.GinkgoTB()
			client := tgt.pickNode(0)
			if tgt.isCluster {
				probe := tgt.name + "-tag-mp-probe"
				tgt.createBkt(tb, probe)
				ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
				defer cancel()
				waitForMultipartListingCreate(tb, ctx, client, probe, multipartListingKey, 120*time.Second)
			}
		})
		runObjectTaggingCases(func() s3Target { return tgt })
	})
}

func runObjectTaggingCases(getTgt func() s3Target) {
	ginkgo.It("Put/Get/Delete round-trip (PutGetDelete)", func(ctx context.Context) {
		tgt := getTgt()
		client := tgt.pickNode(0)
		bucket := tgt.uniqueBucket(ginkgo.GinkgoTB(), "putget")
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket), Key: aws.String("k"),
			Body: stringReader("hello"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		_, err = client.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{
			Bucket: aws.String(bucket), Key: aws.String("k"),
			Tagging: &types.Tagging{TagSet: []types.Tag{
				{Key: aws.String("env"), Value: aws.String("prod")},
				{Key: aws.String("owner"), Value: aws.String("alice")},
			}},
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		got, err := client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
			Bucket: aws.String(bucket), Key: aws.String("k"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(got.TagSet).To(gomega.HaveLen(2))

		_, err = client.DeleteObjectTagging(ctx, &s3.DeleteObjectTaggingInput{
			Bucket: aws.String(bucket), Key: aws.String("k"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		got, err = client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
			Bucket: aws.String(bucket), Key: aws.String("k"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(got.TagSet).To(gomega.BeEmpty())
	}, ginkgo.NodeTimeout(120*time.Second))

	ginkgo.It("materialises ?tagging header on PUT (HeaderOnPut)", func(ctx context.Context) {
		tgt := getTgt()
		client := tgt.pickNode(0)
		bucket := tgt.uniqueBucket(ginkgo.GinkgoTB(), "hdr")
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket), Key: aws.String("k"),
			Body: stringReader("body"), Tagging: aws.String("env=prod&owner=alice"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		got, err := client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
			Bucket: aws.String(bucket), Key: aws.String("k"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(got.TagSet).To(gomega.HaveLen(2))
	}, ginkgo.NodeTimeout(120*time.Second))

	ginkgo.It("rejects reserved aws:* prefix in PUT ?tagging (HeaderOnPut_InvalidRejected)", func(ctx context.Context) {
		tgt := getTgt()
		client := tgt.pickNode(0)
		bucket := tgt.uniqueBucket(ginkgo.GinkgoTB(), "hdrbad")
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket), Key: aws.String("k"),
			Body: stringReader("body"), Tagging: aws.String("aws:env=prod"),
		})
		gomega.Expect(err).To(gomega.HaveOccurred())
		var apiErr smithy.APIError
		gomega.Expect(errors.As(err, &apiErr)).To(gomega.BeTrue())
		gomega.Expect(apiErr.ErrorCode()).To(gomega.Equal("InvalidTag"))
	}, ginkgo.NodeTimeout(120*time.Second))

	ginkgo.It("materialises tags on CompleteMultipartUpload (MultipartCreate_TagsMaterialiseOnComplete)", func(ctx context.Context) {
		tgt := getTgt()
		client := tgt.pickNode(0)
		bucket := tgt.uniqueBucket(ginkgo.GinkgoTB(), "mpu")

		created, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucket), Key: aws.String("k"),
			Tagging: aws.String("env=prod"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Use the shortest legal part (>=5 MiB is the S3 minimum except the
		// last part).
		uploadOnePartAndComplete(ginkgo.GinkgoTB(), ctx, client, bucket, "k", *created.UploadId)

		got, err := client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
			Bucket: aws.String(bucket), Key: aws.String("k"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(got.TagSet).To(gomega.HaveLen(1))
		gomega.Expect(aws.ToString(got.TagSet[0].Key)).To(gomega.Equal("env"))
	}, ginkgo.NodeTimeout(180*time.Second))

	ginkgo.It("returns NoSuchKey on PutObjectTagging against incomplete MPU (PutObjectTagging_OnIncompleteMPU_404)", func(ctx context.Context) {
		tgt := getTgt()
		client := tgt.pickNode(0)
		bucket := tgt.uniqueBucket(ginkgo.GinkgoTB(), "incomplete")

		_, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucket), Key: aws.String("k"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		_, err = client.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{
			Bucket: aws.String(bucket), Key: aws.String("k"),
			Tagging: &types.Tagging{TagSet: []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}}},
		})
		gomega.Expect(err).To(gomega.HaveOccurred())
		var apiErr smithy.APIError
		gomega.Expect(errors.As(err, &apiErr)).To(gomega.BeTrue())
		gomega.Expect(apiErr.ErrorCode()).To(gomega.Equal("NoSuchKey"))
	}, ginkgo.NodeTimeout(120*time.Second))

	ginkgo.It("respects REPLACE TaggingDirective on CopyObject (CopyObject_ReplaceDirective)", func(ctx context.Context) {
		tgt := getTgt()
		client := tgt.pickNode(0)
		src := tgt.uniqueBucket(ginkgo.GinkgoTB(), "src")
		dst := tgt.uniqueBucket(ginkgo.GinkgoTB(), "dst")

		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(src), Key: aws.String("k"),
			Body: stringReader("body"), Tagging: aws.String("old=1"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		_, err = client.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket: aws.String(dst), Key: aws.String("k"),
			CopySource:       aws.String(src + "/k"),
			TaggingDirective: types.TaggingDirectiveReplace,
			Tagging:          aws.String("new=2"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		got, err := client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
			Bucket: aws.String(dst), Key: aws.String("k"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(got.TagSet).To(gomega.HaveLen(1))
		gomega.Expect(aws.ToString(got.TagSet[0].Key)).To(gomega.Equal("new"))
	}, ginkgo.NodeTimeout(120*time.Second))

	ginkgo.It("scopes tags per version (Versioning_PerVersion)", func(ctx context.Context) {
		tgt := getTgt()
		if !tgt.isCluster {
			ginkgo.Skip("Local single-node storage does not implement versionID-aware object tag mutation; cluster-only.")
		}
		client := tgt.pickNode(0)
		bucket := tgt.uniqueBucket(ginkgo.GinkgoTB(), "ver")
		_, err := client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
			Bucket:                  aws.String(bucket),
			VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		v1, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket), Key: aws.String("k"),
			Body: stringReader("v1"), Tagging: aws.String("v=1"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket), Key: aws.String("k"),
			Body: stringReader("v2"), Tagging: aws.String("v=2"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		t1, err := client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
			Bucket: aws.String(bucket), Key: aws.String("k"), VersionId: v1.VersionId,
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(t1.TagSet).To(gomega.HaveLen(1))
		gomega.Expect(aws.ToString(t1.TagSet[0].Value)).To(gomega.Equal("1"))

		t2, err := client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
			Bucket: aws.String(bucket), Key: aws.String("k"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(t2.TagSet).To(gomega.HaveLen(1))
		gomega.Expect(aws.ToString(t2.TagSet[0].Value)).To(gomega.Equal("2"))
	}, ginkgo.NodeTimeout(120*time.Second))
}

// uploadOnePartAndComplete uploads a single 5 MiB part (the S3 multipart
// minimum for non-last parts) and completes the multipart upload.
func uploadOnePartAndComplete(t testing.TB, ctx context.Context, client *s3.Client, bucket, key, uploadID string) {
	t.Helper()
	body := bytes.Repeat([]byte{'x'}, 5*1024*1024) // 5 MiB
	partResp, err := client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String(key),
		UploadId:   aws.String(uploadID),
		PartNumber: aws.Int32(1),
		Body:       bytes.NewReader(body),
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{{ETag: partResp.ETag, PartNumber: aws.Int32(1)}},
		},
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}
