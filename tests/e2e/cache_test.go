package e2e

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCacheE2E probes the cache-coherence surface (PUT-overwrite, DELETE,
// HEAD-after-PUT) at the S3 API level. The cache itself is internal; we only
// assert subsequent reads see fresh data. Shared single + shared cluster
// fixtures, each sub-test gets its own bucket via uniqueBucket.
func TestCacheE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runCacheCases(t, newSingleNodeS3Target())
	})
	t.Run("Cluster4Node", func(t *testing.T) {
		runCacheCases(t, newSharedClusterS3Target(t))
	})
}

func runCacheCases(t *testing.T, tgt s3Target) {
	t.Helper()
	ctx := context.Background()
	cli := tgt.pickNode(0)

	t.Run("ReadConsistency", func(t *testing.T) {
		bucket := tgt.uniqueBucket(t, "cacheread")
		key := "cached-key"
		body := "cache-test-data-v1"

		_, err := cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(bucket),
			Key:         aws.String(key),
			Body:        strings.NewReader(body),
			ContentType: aws.String("text/plain"),
		})
		require.NoError(t, err)

		for i := 0; i < 2; i++ {
			resp, err := cli.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			})
			require.NoError(t, err)
			data, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			assert.Equal(t, body, string(data))
		}

		bodyV2 := "cache-test-data-v2"
		_, err = cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(bucket),
			Key:         aws.String(key),
			Body:        strings.NewReader(bodyV2),
			ContentType: aws.String("text/plain"),
		})
		require.NoError(t, err)

		resp, err := cli.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
		data, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		assert.Equal(t, bodyV2, string(data))
	})

	t.Run("DeleteInvalidation", func(t *testing.T) {
		bucket := tgt.uniqueBucket(t, "cachedel")
		key := "del-key"

		_, err := cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   strings.NewReader("to-be-deleted"),
		})
		require.NoError(t, err)

		resp, err := cli.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
		resp.Body.Close()

		_, err = cli.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)

		_, err = cli.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		assert.Error(t, err)
	})

	t.Run("HeadAfterPut", func(t *testing.T) {
		bucket := tgt.uniqueBucket(t, "cachehead")
		key := "head-key"

		_, err := cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   strings.NewReader("short"),
		})
		require.NoError(t, err)

		head1, err := cli.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
		assert.Equal(t, int64(5), *head1.ContentLength)

		_, err = cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   strings.NewReader("much longer content"),
		})
		require.NoError(t, err)

		head2, err := cli.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
		assert.Equal(t, int64(19), *head2.ContentLength)
	})
}
