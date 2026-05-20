// Bucket-level S3 API e2e (target table-driven).
//
// The same case set runs against a single-node fixture and a 4-node cluster
// fixture. Bucket names are derived from t.Name()+case via tgt.uniqueBucket
// to avoid namespace collisions in the shared cluster fixture.
package e2e

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBucketsE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runBucketCases(t, newSingleNodeS3Target())
	})

	t.Run("Cluster4Node", func(t *testing.T) {
		runBucketCases(t, newSharedClusterS3Target(t))
	})
}

func runBucketCases(t *testing.T, tgt s3Target) {
	client := tgt.pickNode(0)

	t.Run("Create", func(t *testing.T) {
		// uniqueBucket exercises CreateBucket internally and registers
		// auto-cleanup; reaching this line means creation succeeded.
		_ = tgt.uniqueBucket(t, "create")
	})

	t.Run("Head", func(t *testing.T) {
		ctx := context.Background()
		name := tgt.uniqueBucket(t, "head")

		_, err := client.HeadBucket(ctx, &s3.HeadBucketInput{
			Bucket: aws.String(name),
		})
		require.NoError(t, err)
	})

	t.Run("HeadNotFound", func(t *testing.T) {
		ctx := context.Background()
		// Synthesize a name that won't collide with any uniqueBucket.
		name := tgt.name + "-headnotfound-missing"
		_, err := client.HeadBucket(ctx, &s3.HeadBucketInput{
			Bucket: aws.String(name),
		})
		require.Error(t, err)

		var apiErr smithy.APIError
		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, "NotFound", apiErr.ErrorCode())
	})

	t.Run("CreateDeniedOnDataPlane", func(t *testing.T) {
		ctx := context.Background()
		name := tgt.uniqueBucket(t, "conflict")

		_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String(name),
		})
		require.Error(t, err)

		var apiErr smithy.APIError
		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, "AccessDenied", apiErr.ErrorCode())
	})

	t.Run("List", func(t *testing.T) {
		ctx := context.Background()
		name := tgt.uniqueBucket(t, "list")

		out, err := client.ListBuckets(ctx, &s3.ListBucketsInput{})
		require.NoError(t, err)

		found := false
		for _, b := range out.Buckets {
			if aws.ToString(b.Name) == name {
				found = true
				break
			}
		}
		assert.True(t, found, "bucket %q should appear in ListBuckets", name)
	})

	t.Run("Delete", func(t *testing.T) {
		ctx := context.Background()
		name := tgt.uniqueBucket(t, "delete")

		_, err := client.DeleteBucket(ctx, &s3.DeleteBucketInput{
			Bucket: aws.String(name),
		})
		require.Error(t, err)

		var apiErr smithy.APIError
		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, "AccessDenied", apiErr.ErrorCode())
	})

	t.Run("DeleteNotEmpty", func(t *testing.T) {
		ctx := context.Background()
		name := tgt.uniqueBucket(t, "notempty")

		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(name),
			Key:    aws.String("file.txt"),
			Body:   stringReader("data"),
		})
		require.NoError(t, err)

		_, err = client.DeleteBucket(ctx, &s3.DeleteBucketInput{
			Bucket: aws.String(name),
		})
		require.Error(t, err)

		var apiErr smithy.APIError
		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, "AccessDenied", apiErr.ErrorCode())
	})

	t.Run("InternalBucketDeniedAndNotListed", func(t *testing.T) {
		ctx := context.Background()

		// Data-plane bucket lifecycle is admin-UDS-only; internal buckets are
		// also rejected before storage creation.
		internalName := "__grainfs_review_test_internal_" + tgt.name
		_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(internalName)})
		require.Error(t, err)

		out, err := client.ListBuckets(ctx, &s3.ListBucketsInput{})
		require.NoError(t, err)
		for _, b := range out.Buckets {
			name := aws.ToString(b.Name)
			if strings.HasPrefix(name, "__grainfs_") {
				t.Errorf("ListBuckets exposed internal bucket %q", name)
			}
		}
	})
}
