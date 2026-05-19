package e2e

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestECObjectsE2E exercises the default storage path under the project's
// standard erasure-coded layout (Reed-Solomon 4+2). Five bucket-isolated S3
// surface cases ride the shared single + shared cluster fixtures.
//
// Any per-test cluster topology mutation belongs in cluster_test.go behind
// newClusterS3Target — this group stays bucket-isolated.
func TestECObjectsE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runECObjectsCases(t, newSingleNodeS3Target())
	})
	t.Run("Cluster4Node", func(t *testing.T) {
		runECObjectsCases(t, newSharedClusterS3Target(t))
	})
}

func runECObjectsCases(t *testing.T, tgt s3Target) {
	t.Helper()
	ctx := context.Background()
	cli := tgt.pickNode(0)

	t.Run("BasicPutGet", func(t *testing.T) {
		bucket := tgt.uniqueBucket(t, "basic")
		cases := []struct {
			name    string
			key     string
			content string
		}{
			{"small_object", "small.txt", "hello erasure coding"},
			{"medium_object", "medium.txt", strings.Repeat("EC test data ", 1000)},
			{"nested_key", "path/to/deep/file.txt", "nested EC content"},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				_, err := cli.PutObject(ctx, &s3.PutObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(tc.key),
					Body:   strings.NewReader(tc.content),
				})
				require.NoError(t, err)

				getOut, err := cli.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(tc.key),
				})
				require.NoError(t, err)
				defer getOut.Body.Close()

				body, _ := io.ReadAll(getOut.Body)
				assert.Equal(t, tc.content, string(body))
				assert.Equal(t, int64(len(tc.content)), aws.ToInt64(getOut.ContentLength))
			})
		}
	})

	t.Run("LargeObject", func(t *testing.T) {
		bucket := tgt.uniqueBucket(t, "large")
		// 5MiB body — exceeds the default shard size, forcing a true EC stripe.
		data := bytes.Repeat([]byte("X"), 5*1024*1024)
		_, err := cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("large.bin"),
			Body:   bytes.NewReader(data),
		})
		require.NoError(t, err)

		getOut, err := cli.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("large.bin"),
		})
		require.NoError(t, err)
		defer getOut.Body.Close()

		body, _ := io.ReadAll(getOut.Body)
		assert.Equal(t, data, body)
	})

	t.Run("MultipartUpload", func(t *testing.T) {
		bucket := tgt.uniqueBucket(t, "multipart")
		key := "multipart-ec.bin"
		part1Data := bytes.Repeat([]byte("A"), 1024)
		part2Data := bytes.Repeat([]byte("B"), 512)

		initOut, err := cli.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)

		p1, err := cli.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String(key),
			UploadId:   initOut.UploadId,
			PartNumber: aws.Int32(1),
			Body:       bytes.NewReader(part1Data),
		})
		require.NoError(t, err)

		p2, err := cli.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String(key),
			UploadId:   initOut.UploadId,
			PartNumber: aws.Int32(2),
			Body:       bytes.NewReader(part2Data),
		})
		require.NoError(t, err)

		_, err = cli.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(bucket),
			Key:      aws.String(key),
			UploadId: initOut.UploadId,
			MultipartUpload: &types.CompletedMultipartUpload{
				Parts: []types.CompletedPart{
					{PartNumber: aws.Int32(1), ETag: p1.ETag},
					{PartNumber: aws.Int32(2), ETag: p2.ETag},
				},
			},
		})
		require.NoError(t, err)

		getOut, err := cli.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
		defer getOut.Body.Close()

		body, _ := io.ReadAll(getOut.Body)
		expected := append(part1Data, part2Data...)
		assert.Equal(t, expected, body)
	})

	t.Run("BucketOperations", func(t *testing.T) {
		bucket := tgt.uniqueBucket(t, "bktops")

		_, err := cli.HeadBucket(ctx, &s3.HeadBucketInput{
			Bucket: aws.String(bucket),
		})
		require.NoError(t, err)

		listOut, err := cli.ListBuckets(ctx, &s3.ListBucketsInput{})
		require.NoError(t, err)
		found := false
		for _, b := range listOut.Buckets {
			if aws.ToString(b.Name) == bucket {
				found = true
				break
			}
		}
		assert.True(t, found, "newly created bucket %s missing from ListBuckets", bucket)

		// Empty bucket delete may race with routed ListObjects readiness on
		// cluster, so retry within the same 30s envelope the standalone test used.
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			_, err = cli.DeleteBucket(ctx, &s3.DeleteBucketInput{
				Bucket: aws.String(bucket),
			})
			assert.NoError(c, err)
		}, 30*time.Second, 250*time.Millisecond, "empty bucket deletion should wait for routed ListObjects readiness")
	})

	t.Run("DeleteAndOverwrite", func(t *testing.T) {
		bucket := tgt.uniqueBucket(t, "delover")

		_, err := cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("file.txt"),
			Body:   strings.NewReader("v1"),
		})
		require.NoError(t, err)

		_, err = cli.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("file.txt"),
		})
		require.NoError(t, err)

		_, err = cli.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("file.txt"),
		})
		assert.Error(t, err)

		_, err = cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("over.txt"),
			Body:   strings.NewReader("version1"),
		})
		require.NoError(t, err)

		_, err = cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("over.txt"),
			Body:   strings.NewReader("version2"),
		})
		require.NoError(t, err)

		getOut, err := cli.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("over.txt"),
		})
		require.NoError(t, err)
		defer getOut.Body.Close()

		body, _ := io.ReadAll(getOut.Body)
		assert.Equal(t, "version2", string(body))
	})
}
