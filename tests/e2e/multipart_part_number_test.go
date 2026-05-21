// GET/HEAD ?partNumber=N e2e for multipart objects (PR #430 follow-up).
//
// Reproduces warp multipart workload failures observed against the 4-node
// cluster:
//   - "the requested partnumber is not satisfiable for this object"
//   - "unexpected download size. want:5242880, got:5250166"
//
// Runs SingleNode + Cluster4Node to localize the bug (single-node Parts
// persistence vs cluster EC/spool framing).
package e2e

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

const multipartPartSize = 5 * 1024 * 1024 // 5 MiB — warp's minimum part.size

func runMultipartGetPartNumberSpecs() {
	ginkgo.Context("GetPartNumber SingleNode", func() {
		var tgt s3Target
		ginkgo.BeforeEach(func() {
			tgt = newSingleNodeS3Target()
		})
		runMultipartGetPartNumberCases(func() s3Target { return tgt })
	})

	ginkgo.Context("GetPartNumber Cluster4Node", func() {
		var tgt s3Target
		ginkgo.BeforeEach(func() {
			tgt = newSharedClusterS3Target(ginkgo.GinkgoTB())
		})
		runMultipartGetPartNumberCases(func() s3Target { return tgt })
	})
}

func runMultipartGetPartNumberCases(getTgt func() s3Target) {
	ginkgo.It("serves full and numbered GETs for two equal 5MiB parts", func() {
		t, tgt, client := multipartFixture(getTgt, "mp-pn-probe")
		ctx := context.Background()
		bucket := tgt.uniqueBucket(t, "mp-pn-two")
		key := "warp-multipart.bin"

		part1 := bytes.Repeat([]byte{'A'}, multipartPartSize)
		part2 := bytes.Repeat([]byte{'B'}, multipartPartSize)

		initOut, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket:      aws.String(bucket),
			Key:         aws.String(key),
			ContentType: aws.String("application/octet-stream"),
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

		// Full GET — must return concatenated plaintext, no encryption overhead leaked.
		out, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer out.Body.Close()
		body, err := io.ReadAll(out.Body)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		expected := append(append([]byte{}, part1...), part2...)
		gomega.Expect(aws.ToInt64(out.ContentLength)).To(gomega.Equal(int64(len(expected))), "Content-Length header")
		gomega.Expect(len(body)).To(gomega.Equal(len(expected)), "body byte length")
		if len(body) == len(expected) {
			gomega.Expect(body[:32]).To(gomega.Equal(expected[:32]), "first 32 bytes")
			gomega.Expect(body[len(body)-32:]).To(gomega.Equal(expected[len(expected)-32:]), "last 32 bytes")
		}

		assertPart(t, client, ctx, bucket, key, 1, part1, 2)
		assertPart(t, client, ctx, bucket, key, 2, part2, 2)

		_, err = client.GetObject(ctx, &s3.GetObjectInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String(key),
			PartNumber: aws.Int32(3),
		})
		gomega.Expect(err).To(gomega.HaveOccurred(), "partNumber > parts count must error")
	})
}

func assertPart(t testing.TB, client *s3.Client, ctx context.Context, bucket, key string, partN int32, expected []byte, totalParts int32) {
	t.Helper()
	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String(key),
		PartNumber: aws.Int32(partN),
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer out.Body.Close()

	gomega.Expect(aws.ToInt64(out.ContentLength)).To(gomega.Equal(int64(len(expected))), "part %d Content-Length", partN)
	gomega.Expect(aws.ToInt32(out.PartsCount)).To(gomega.Equal(totalParts), "part %d x-amz-mp-parts-count", partN)

	body, err := io.ReadAll(out.Body)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "part %d ReadAll", partN)
	gomega.Expect(len(body)).To(gomega.Equal(len(expected)), "part %d body length", partN)
	if len(body) == len(expected) {
		gomega.Expect(body[:32]).To(gomega.Equal(expected[:32]), "part %d first 32 bytes", partN)
		gomega.Expect(body[len(body)-32:]).To(gomega.Equal(expected[len(expected)-32:]), "part %d last 32 bytes", partN)
	}
}
