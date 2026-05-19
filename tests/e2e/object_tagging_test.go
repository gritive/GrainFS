package e2e

import (
	"bytes"
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestObjectTaggingE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runObjectTaggingCases(t, newSingleNodeS3Target())
	})
	t.Run("Cluster4Node", func(t *testing.T) {
		runObjectTaggingCases(t, newSharedClusterS3Target(t))
	})
}

func runObjectTaggingCases(t *testing.T, tgt s3Target) {
	client := tgt.pickNode(0)

	t.Run("PutGetDelete", func(t *testing.T) {
		ctx := context.Background()
		bucket := tgt.uniqueBucket(t, "putget")
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket), Key: aws.String("k"),
			Body: stringReader("hello"),
		})
		require.NoError(t, err)

		_, err = client.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{
			Bucket: aws.String(bucket), Key: aws.String("k"),
			Tagging: &types.Tagging{TagSet: []types.Tag{
				{Key: aws.String("env"), Value: aws.String("prod")},
				{Key: aws.String("owner"), Value: aws.String("alice")},
			}},
		})
		require.NoError(t, err)

		got, err := client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
			Bucket: aws.String(bucket), Key: aws.String("k"),
		})
		require.NoError(t, err)
		require.Len(t, got.TagSet, 2)

		_, err = client.DeleteObjectTagging(ctx, &s3.DeleteObjectTaggingInput{
			Bucket: aws.String(bucket), Key: aws.String("k"),
		})
		require.NoError(t, err)

		got, err = client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
			Bucket: aws.String(bucket), Key: aws.String("k"),
		})
		require.NoError(t, err)
		assert.Empty(t, got.TagSet)
	})

	t.Run("HeaderOnPut", func(t *testing.T) {
		ctx := context.Background()
		bucket := tgt.uniqueBucket(t, "hdr")
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket), Key: aws.String("k"),
			Body: stringReader("body"), Tagging: aws.String("env=prod&owner=alice"),
		})
		require.NoError(t, err)
		got, err := client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
			Bucket: aws.String(bucket), Key: aws.String("k"),
		})
		require.NoError(t, err)
		require.Len(t, got.TagSet, 2)
	})

	t.Run("HeaderOnPut_InvalidRejected", func(t *testing.T) {
		ctx := context.Background()
		bucket := tgt.uniqueBucket(t, "hdrbad")
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket), Key: aws.String("k"),
			Body: stringReader("body"), Tagging: aws.String("aws:env=prod"),
		})
		require.Error(t, err)
		var apiErr smithy.APIError
		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, "InvalidTag", apiErr.ErrorCode())
	})

	t.Run("MultipartCreate_TagsMaterialiseOnComplete", func(t *testing.T) {
		ctx := context.Background()
		bucket := tgt.uniqueBucket(t, "mpu")

		created, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucket), Key: aws.String("k"),
			Tagging: aws.String("env=prod"),
		})
		require.NoError(t, err)

		// Use the shortest legal part (>=5 MiB is the S3 minimum except the
		// last part). The existing e2e harness has a multipart helper —
		// reuse it if available; otherwise UploadPart inline with 5 MiB.
		uploadOnePartAndComplete(t, ctx, client, bucket, "k", *created.UploadId)

		got, err := client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
			Bucket: aws.String(bucket), Key: aws.String("k"),
		})
		require.NoError(t, err)
		require.Len(t, got.TagSet, 1)
		assert.Equal(t, "env", aws.ToString(got.TagSet[0].Key))
	})

	t.Run("PutObjectTagging_OnIncompleteMPU_404", func(t *testing.T) {
		ctx := context.Background()
		bucket := tgt.uniqueBucket(t, "incomplete")

		_, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucket), Key: aws.String("k"),
		})
		require.NoError(t, err)

		_, err = client.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{
			Bucket: aws.String(bucket), Key: aws.String("k"),
			Tagging: &types.Tagging{TagSet: []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}}},
		})
		require.Error(t, err)
		var apiErr smithy.APIError
		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, "NoSuchKey", apiErr.ErrorCode())
	})

	t.Run("CopyObject_ReplaceDirective", func(t *testing.T) {
		ctx := context.Background()
		src := tgt.uniqueBucket(t, "src")
		dst := tgt.uniqueBucket(t, "dst")

		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(src), Key: aws.String("k"),
			Body: stringReader("body"), Tagging: aws.String("old=1"),
		})
		require.NoError(t, err)

		_, err = client.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket: aws.String(dst), Key: aws.String("k"),
			CopySource:       aws.String(src + "/k"),
			TaggingDirective: types.TaggingDirectiveReplace,
			Tagging:          aws.String("new=2"),
		})
		require.NoError(t, err)

		got, err := client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
			Bucket: aws.String(dst), Key: aws.String("k"),
		})
		require.NoError(t, err)
		require.Len(t, got.TagSet, 1)
		assert.Equal(t, "new", aws.ToString(got.TagSet[0].Key))
	})

	t.Run("Versioning_PerVersion", func(t *testing.T) {
		ctx := context.Background()
		bucket := tgt.uniqueBucket(t, "ver")
		_, err := client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
			Bucket:                  aws.String(bucket),
			VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
		})
		if !tgt.isCluster {
			// LocalBackend does not implement BucketVersioner; cluster-only feature.
			require.Error(t, err)
			return
		}
		require.NoError(t, err)

		v1, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket), Key: aws.String("k"),
			Body: stringReader("v1"), Tagging: aws.String("v=1"),
		})
		require.NoError(t, err)

		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket), Key: aws.String("k"),
			Body: stringReader("v2"), Tagging: aws.String("v=2"),
		})
		require.NoError(t, err)

		t1, err := client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
			Bucket: aws.String(bucket), Key: aws.String("k"), VersionId: v1.VersionId,
		})
		require.NoError(t, err)
		require.Len(t, t1.TagSet, 1)
		assert.Equal(t, "1", aws.ToString(t1.TagSet[0].Value))

		t2, err := client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
			Bucket: aws.String(bucket), Key: aws.String("k"),
		})
		require.NoError(t, err)
		require.Len(t, t2.TagSet, 1)
		assert.Equal(t, "2", aws.ToString(t2.TagSet[0].Value))
	})
}

// uploadOnePartAndComplete uploads a single 5 MiB part (the S3 multipart
// minimum for non-last parts) and completes the multipart upload.
func uploadOnePartAndComplete(t *testing.T, ctx context.Context, client *s3.Client, bucket, key, uploadID string) {
	t.Helper()
	body := bytes.Repeat([]byte{'x'}, 5*1024*1024) // 5 MiB
	partResp, err := client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String(key),
		UploadId:   aws.String(uploadID),
		PartNumber: aws.Int32(1),
		Body:       bytes.NewReader(body),
	})
	require.NoError(t, err)
	_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{{ETag: partResp.ETag, PartNumber: aws.Int32(1)}},
		},
	})
	require.NoError(t, err)
}
