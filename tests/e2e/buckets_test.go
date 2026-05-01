package e2e

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuckets_Create(t *testing.T) {
	ctx := context.Background()
	_, err := testS3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String("create-test"),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		testS3Client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String("create-test")})
	})
}

func TestBuckets_Head(t *testing.T) {
	ctx := context.Background()
	createBucket(t, "head-test")

	_, err := testS3Client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String("head-test"),
	})
	require.NoError(t, err)
}

func TestBuckets_HeadNotFound(t *testing.T) {
	ctx := context.Background()
	_, err := testS3Client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String("nonexistent-bucket"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "NotFound", apiErr.ErrorCode())
}

func TestBuckets_CreateConflict(t *testing.T) {
	ctx := context.Background()
	createBucket(t, "conflict-test")

	_, err := testS3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String("conflict-test"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "BucketAlreadyOwnedByYou", apiErr.ErrorCode())
}

func TestBuckets_List(t *testing.T) {
	ctx := context.Background()
	createBucket(t, "list-test")

	out, err := testS3Client.ListBuckets(ctx, &s3.ListBucketsInput{})
	require.NoError(t, err)

	found := false
	for _, b := range out.Buckets {
		if aws.ToString(b.Name) == "list-test" {
			found = true
			break
		}
	}
	assert.True(t, found, "bucket list-test should appear in ListBuckets")
}

func TestBuckets_Delete(t *testing.T) {
	ctx := context.Background()
	_, err := testS3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String("delete-test"),
	})
	require.NoError(t, err)

	_, err = testS3Client.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String("delete-test"),
	})
	require.NoError(t, err)
}

func TestBuckets_DeleteNotEmpty(t *testing.T) {
	ctx := context.Background()
	createBucket(t, "notempty-test")

	_, err := testS3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("notempty-test"),
		Key:    aws.String("file.txt"),
		Body:   stringReader("data"),
	})
	require.NoError(t, err)

	_, err = testS3Client.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String("notempty-test"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "BucketNotEmpty", apiErr.ErrorCode())
}
