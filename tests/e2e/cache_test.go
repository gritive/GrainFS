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

// Cache tests probe the cache-coherence surface (PUT-overwrite, DELETE,
// HEAD-after-PUT) at the S3 API level. The cache itself is internal; from
// the outside we only assert that subsequent reads see fresh data. That
// invariant holds on both single-node and cluster targets, so each test
// runs the standard TestBucketsE2E dual pattern via runCache{Name}Cases.

func TestCacheReadConsistencyE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runCacheReadConsistencyCases(t, newSingleNodeS3Target())
	})
	t.Run("Cluster4Node", func(t *testing.T) {
		runCacheReadConsistencyCases(t, newSharedClusterS3Target(t))
	})
}

func runCacheReadConsistencyCases(t *testing.T, tgt s3Target) {
	t.Helper()
	ctx := context.Background()
	bucket := tgt.uniqueBucket(t, "cacheread")
	cli := tgt.pickNode(0)
	key := "cached-key"

	body := "cache-test-data-v1"
	_, err := cli.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        strings.NewReader(body),
		ContentType: aws.String("text/plain"),
	})
	require.NoError(t, err)

	resp1, err := cli.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	data1, _ := io.ReadAll(resp1.Body)
	resp1.Body.Close()
	assert.Equal(t, body, string(data1))

	resp2, err := cli.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	data2, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	assert.Equal(t, body, string(data2))

	bodyV2 := "cache-test-data-v2"
	_, err = cli.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        strings.NewReader(bodyV2),
		ContentType: aws.String("text/plain"),
	})
	require.NoError(t, err)

	resp3, err := cli.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	data3, _ := io.ReadAll(resp3.Body)
	resp3.Body.Close()
	assert.Equal(t, bodyV2, string(data3))
}

func TestCacheDeleteInvalidationE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runCacheDeleteInvalidationCases(t, newSingleNodeS3Target())
	})
	t.Run("Cluster4Node", func(t *testing.T) {
		runCacheDeleteInvalidationCases(t, newSharedClusterS3Target(t))
	})
}

func runCacheDeleteInvalidationCases(t *testing.T, tgt s3Target) {
	t.Helper()
	ctx := context.Background()
	bucket := tgt.uniqueBucket(t, "cachedel")
	cli := tgt.pickNode(0)
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
}

func TestCacheHeadAfterPutE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runCacheHeadAfterPutCases(t, newSingleNodeS3Target())
	})
	t.Run("Cluster4Node", func(t *testing.T) {
		runCacheHeadAfterPutCases(t, newSharedClusterS3Target(t))
	})
}

func runCacheHeadAfterPutCases(t *testing.T, tgt s3Target) {
	t.Helper()
	ctx := context.Background()
	bucket := tgt.uniqueBucket(t, "cachehead")
	cli := tgt.pickNode(0)
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
}
