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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		require.NoError(t, err)
		uploadID := initOut.UploadId

		p1, err := client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String(key),
			UploadId:   uploadID,
			PartNumber: aws.Int32(1),
			Body:       bytes.NewReader(part1),
		})
		require.NoError(t, err)

		p2, err := client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String(key),
			UploadId:   uploadID,
			PartNumber: aws.Int32(2),
			Body:       bytes.NewReader(part2),
		})
		require.NoError(t, err)

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
		require.NoError(t, err)

		// Full GET — must return concatenated plaintext, no encryption overhead leaked.
		out, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
		defer out.Body.Close()
		body, err := io.ReadAll(out.Body)
		require.NoError(t, err)
		expected := append(append([]byte{}, part1...), part2...)
		assert.Equal(t, int64(len(expected)), aws.ToInt64(out.ContentLength), "Content-Length header")
		assert.Equal(t, len(expected), len(body), "body byte length")
		if len(body) == len(expected) {
			assert.Equal(t, expected[:32], body[:32], "first 32 bytes")
			assert.Equal(t, expected[len(expected)-32:], body[len(body)-32:], "last 32 bytes")
		}

		assertPart(t, client, ctx, bucket, key, 1, part1, 2)
		assertPart(t, client, ctx, bucket, key, 2, part2, 2)

		_, err = client.GetObject(ctx, &s3.GetObjectInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String(key),
			PartNumber: aws.Int32(3),
		})
		require.Error(t, err, "partNumber > parts count must error")
	})
}

func assertPart(t testing.TB, client *s3.Client, ctx context.Context, bucket, key string, partN int32, expected []byte, totalParts int32) {
	t.Helper()
	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String(key),
		PartNumber: aws.Int32(partN),
	})
	require.NoError(t, err)
	defer out.Body.Close()

	assert.Equal(t, int64(len(expected)), aws.ToInt64(out.ContentLength), "part %d Content-Length", partN)
	assert.Equal(t, totalParts, aws.ToInt32(out.PartsCount), "part %d x-amz-mp-parts-count", partN)

	body, err := io.ReadAll(out.Body)
	require.NoError(t, err, "part %d ReadAll", partN)
	assert.Equal(t, len(expected), len(body), "part %d body length", partN)
	if len(body) == len(expected) {
		assert.Equal(t, expected[:32], body[:32], "part %d first 32 bytes", partN)
		assert.Equal(t, expected[len(expected)-32:], body[len(body)-32:], "part %d last 32 bytes", partN)
	}
}
