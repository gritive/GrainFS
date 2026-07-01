// UploadPartCopy (multipart server-side copy) S3 API e2e.
//
// Regression coverage for the handlePut mis-routing trap: a
// PUT /b/k?partNumber=N&uploadId=ID + x-amz-copy-source request was handled
// as a plain UploadPart, so the copy source was silently dropped and an empty
// part was stored (CRITICAL silent data loss on large-object server-side copy).
// The same case set runs against a single-node fixture and a 4-node cluster
// fixture, mirroring multipart_test.go.
package e2e

import (
	"bytes"
	"context"
	"errors"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("UploadPartCopy", func() {
	runUploadPartCopySpecs()
})

func runUploadPartCopySpecs() {
	ginkgo.Context("UploadPartCopy SingleNode", func() {
		var tgt s3Target
		ginkgo.BeforeEach(func() {
			tgt = newSingleNodeS3Target()
		})
		runUploadPartCopyCases(func() s3Target { return tgt })
	})

	ginkgo.Context("UploadPartCopy Cluster4Node", func() {
		var tgt s3Target
		ginkgo.BeforeEach(func() {
			tgt = newSharedClusterS3Target(ginkgo.GinkgoTB())
		})
		runUploadPartCopyCases(func() s3Target { return tgt })
	})
}

func runUploadPartCopyCases(getTgt func() s3Target) {
	ginkgo.It("copies a whole-object part via UploadPartCopy", func() {
		t, tgt, client := multipartFixture(getTgt, "upc-probe")
		ctx := context.Background()
		bucket := tgt.uniqueBucket(t, "upc-whole")

		srcKey := "source/whole.bin"
		dstKey := "dest/whole.bin"
		srcData := largeObjectRandomBytes(6 << 20) // 6 MiB
		tailData := bytes.Repeat([]byte("T"), 512)

		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(srcKey),
			Body:   bytes.NewReader(srcData),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		initOut, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(dstKey),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		uploadID := initOut.UploadId

		// Part 1: server-side copy of the whole source object.
		cpOut, err := client.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String(dstKey),
			UploadId:   uploadID,
			PartNumber: aws.Int32(1),
			CopySource: aws.String(bucket + "/" + srcKey),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// RED guard: the misrouted path returns a bare ETag header and no
		// CopyPartResult XML, so the SDK leaves CopyPartResult nil. Assert
		// non-nil before dereferencing so the trap surfaces as a clean
		// assertion failure rather than a nil-pointer panic.
		gomega.Expect(cpOut.CopyPartResult).NotTo(gomega.BeNil())
		gomega.Expect(aws.ToString(cpOut.CopyPartResult.ETag)).NotTo(gomega.BeEmpty())

		// Part 2: plain UploadPart tail (must remain unaffected by the routing fork).
		p2, err := client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String(dstKey),
			UploadId:   uploadID,
			PartNumber: aws.Int32(2),
			Body:       bytes.NewReader(tailData),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(bucket),
			Key:      aws.String(dstKey),
			UploadId: uploadID,
			MultipartUpload: &types.CompletedMultipartUpload{
				Parts: []types.CompletedPart{
					{PartNumber: aws.Int32(1), ETag: cpOut.CopyPartResult.ETag},
					{PartNumber: aws.Int32(2), ETag: p2.ETag},
				},
			},
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		getOut, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(dstKey),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(getOut.Body.Close)

		body, _ := io.ReadAll(getOut.Body)
		expected := append(append([]byte{}, srcData...), tailData...)
		gomega.Expect(body).To(gomega.Equal(expected))
		gomega.Expect(aws.ToInt64(getOut.ContentLength)).To(gomega.Equal(int64(len(expected))))
	})

	ginkgo.It("copies a byte range via UploadPartCopy CopySourceRange", func() {
		t, tgt, client := multipartFixture(getTgt, "upc-probe")
		ctx := context.Background()
		bucket := tgt.uniqueBucket(t, "upc-range")

		srcKey := "source/range.bin"
		dstKey := "dest/range.bin"
		srcData := largeObjectRandomBytes(6 << 20) // 6 MiB

		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(srcKey),
			Body:   bytes.NewReader(srcData),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		initOut, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(dstKey),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		uploadID := initOut.UploadId

		// Part 1: first 5 MiB via CopySourceRange (inclusive end index).
		cp1, err := client.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
			Bucket:          aws.String(bucket),
			Key:             aws.String(dstKey),
			UploadId:        uploadID,
			PartNumber:      aws.Int32(1),
			CopySource:      aws.String(bucket + "/" + srcKey),
			CopySourceRange: aws.String("bytes=0-5242879"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(cp1.CopyPartResult).NotTo(gomega.BeNil())

		// Part 2: remainder via CopySourceRange.
		cp2, err := client.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
			Bucket:          aws.String(bucket),
			Key:             aws.String(dstKey),
			UploadId:        uploadID,
			PartNumber:      aws.Int32(2),
			CopySource:      aws.String(bucket + "/" + srcKey),
			CopySourceRange: aws.String("bytes=5242880-6291455"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(cp2.CopyPartResult).NotTo(gomega.BeNil())

		_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(bucket),
			Key:      aws.String(dstKey),
			UploadId: uploadID,
			MultipartUpload: &types.CompletedMultipartUpload{
				Parts: []types.CompletedPart{
					{PartNumber: aws.Int32(1), ETag: cp1.CopyPartResult.ETag},
					{PartNumber: aws.Int32(2), ETag: cp2.CopyPartResult.ETag},
				},
			},
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		getOut, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(dstKey),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(getOut.Body.Close)

		body, _ := io.ReadAll(getOut.Body)
		gomega.Expect(body).To(gomega.Equal(srcData))
	})

	ginkgo.It("plain UploadPart is unaffected by the copy-source routing change", func() {
		t, tgt, client := multipartFixture(getTgt, "upc-probe")
		ctx := context.Background()
		bucket := tgt.uniqueBucket(t, "upc-plain")

		key := "dest/plain.bin"
		part1 := bytes.Repeat([]byte("A"), 5<<20)
		part2 := bytes.Repeat([]byte("B"), 256)

		initOut, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		uploadID := initOut.UploadId

		p1, err := client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String(key),
			UploadId:   uploadID,
			PartNumber: aws.Int32(1),
			Body:       bytes.NewReader(part1),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		p2, err := client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String(key),
			UploadId:   uploadID,
			PartNumber: aws.Int32(2),
			Body:       bytes.NewReader(part2),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(bucket),
			Key:      aws.String(key),
			UploadId: uploadID,
			MultipartUpload: &types.CompletedMultipartUpload{
				Parts: []types.CompletedPart{
					{PartNumber: aws.Int32(1), ETag: p1.ETag},
					{PartNumber: aws.Int32(2), ETag: p2.ETag},
				},
			},
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		getOut, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(getOut.Body.Close)

		body, _ := io.ReadAll(getOut.Body)
		expected := append(append([]byte{}, part1...), part2...)
		gomega.Expect(body).To(gomega.Equal(expected))
	})

	ginkgo.It("rejects UploadPartCopy from a non-existent source with NoSuchKey", func() {
		t, tgt, client := multipartFixture(getTgt, "upc-probe")
		ctx := context.Background()
		bucket := tgt.uniqueBucket(t, "upc-missing")

		dstKey := "dest/missing-src.bin"
		initOut, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(dstKey),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Existing bucket, missing key → NoSuchKey (a bogus bucket would surface
		// NoSuchBucket instead; keep the source bucket real for determinism).
		_, err = client.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String(dstKey),
			UploadId:   initOut.UploadId,
			PartNumber: aws.Int32(1),
			CopySource: aws.String(bucket + "/does-not-exist.bin"),
		})
		gomega.Expect(err).To(gomega.HaveOccurred())
		var apiErr smithy.APIError
		gomega.Expect(errors.As(err, &apiErr)).To(gomega.BeTrue())
		gomega.Expect(apiErr.ErrorCode()).To(gomega.Equal("NoSuchKey"))
	})

	ginkgo.It("copies a specific source version via UploadPartCopy versionId", func() {
		t, tgt, client := multipartFixture(getTgt, "upc-probe")
		ctx := context.Background()
		bucket := tgt.uniqueBucket(t, "upc-version")

		_, err := client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
			Bucket: aws.String(bucket),
			VersioningConfiguration: &types.VersioningConfiguration{
				Status: types.BucketVersioningStatusEnabled,
			},
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		srcKey := "source/versioned.bin"
		dstKey := "dest/versioned.bin"
		oldData := bytes.Repeat([]byte("O"), 6<<20) // the version we copy
		newData := bytes.Repeat([]byte("N"), 6<<20) // latest; must NOT be copied

		putOld, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(srcKey),
			Body:   bytes.NewReader(oldData),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		oldVersionID := aws.ToString(putOld.VersionId)
		gomega.Expect(oldVersionID).NotTo(gomega.BeEmpty())

		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(srcKey),
			Body:   bytes.NewReader(newData),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		initOut, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(dstKey),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Copy the OLD version explicitly via ?versionId — exercises the
		// GetObjectVersion branch + SourceVersioningCtx stamping.
		cpOut, err := client.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String(dstKey),
			UploadId:   initOut.UploadId,
			PartNumber: aws.Int32(1),
			CopySource: aws.String(bucket + "/" + srcKey + "?versionId=" + oldVersionID),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(cpOut.CopyPartResult).NotTo(gomega.BeNil())

		_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(bucket),
			Key:      aws.String(dstKey),
			UploadId: initOut.UploadId,
			MultipartUpload: &types.CompletedMultipartUpload{
				Parts: []types.CompletedPart{
					{PartNumber: aws.Int32(1), ETag: cpOut.CopyPartResult.ETag},
				},
			},
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		getOut, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(dstKey),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(getOut.Body.Close)

		body, _ := io.ReadAll(getOut.Body)
		// Must equal the OLD version bytes, not the latest — proves the
		// per-version source read, not a latest-read fallback.
		gomega.Expect(body).To(gomega.Equal(oldData))
	})
}
