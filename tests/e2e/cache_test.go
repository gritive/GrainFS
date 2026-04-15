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

// TestCacheReadConsistency verifies that reads through the cache layer
// return correct data, and that writes properly invalidate cached entries.
func TestCacheReadConsistency(t *testing.T) {
	bucket := "cache-e2e-test"
	createBucket(t, bucket)
	ctx := context.Background()

	// Put an object
	body := "cache-test-data-v1"
	_, err := testS3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String("cached-key"),
		Body:        strings.NewReader(body),
		ContentType: aws.String("text/plain"),
	})
	require.NoError(t, err)

	// First GET: cache miss
	resp1, err := testS3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("cached-key"),
	})
	require.NoError(t, err)
	data1, _ := io.ReadAll(resp1.Body)
	resp1.Body.Close()
	assert.Equal(t, body, string(data1))

	// Second GET: cache hit (should return same data)
	resp2, err := testS3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("cached-key"),
	})
	require.NoError(t, err)
	data2, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	assert.Equal(t, body, string(data2))

	// Overwrite with v2
	bodyV2 := "cache-test-data-v2"
	_, err = testS3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String("cached-key"),
		Body:        strings.NewReader(bodyV2),
		ContentType: aws.String("text/plain"),
	})
	require.NoError(t, err)

	// GET after overwrite: cache should be invalidated, return v2
	resp3, err := testS3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("cached-key"),
	})
	require.NoError(t, err)
	data3, _ := io.ReadAll(resp3.Body)
	resp3.Body.Close()
	assert.Equal(t, bodyV2, string(data3))
}

// TestCacheDeleteInvalidation verifies that deleting an object invalidates
// the cache so subsequent reads fail properly.
func TestCacheDeleteInvalidation(t *testing.T) {
	bucket := "cache-del-test"
	createBucket(t, bucket)
	ctx := context.Background()

	_, err := testS3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("del-key"),
		Body:   strings.NewReader("to-be-deleted"),
	})
	require.NoError(t, err)

	// Read to populate cache
	resp, err := testS3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("del-key"),
	})
	require.NoError(t, err)
	resp.Body.Close()

	// Delete
	_, err = testS3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("del-key"),
	})
	require.NoError(t, err)

	// GET after delete should fail
	_, err = testS3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("del-key"),
	})
	assert.Error(t, err)
}

// TestCacheHeadAfterPut verifies that HeadObject returns fresh metadata
// after a PutObject, not stale cached metadata.
func TestCacheHeadAfterPut(t *testing.T) {
	bucket := "cache-head-test"
	createBucket(t, bucket)
	ctx := context.Background()

	// Put small object
	_, err := testS3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("head-key"),
		Body:   strings.NewReader("short"),
	})
	require.NoError(t, err)

	head1, err := testS3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("head-key"),
	})
	require.NoError(t, err)
	assert.Equal(t, int64(5), *head1.ContentLength)

	// Overwrite with larger
	_, err = testS3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("head-key"),
		Body:   strings.NewReader("much longer content"),
	})
	require.NoError(t, err)

	head2, err := testS3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("head-key"),
	})
	require.NoError(t, err)
	assert.Equal(t, int64(19), *head2.ContentLength)
}
