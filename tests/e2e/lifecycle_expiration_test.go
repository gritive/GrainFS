package e2e

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLifecycleExpirationE2E exercises the leader-side expiration path via the
// public S3 lifecycle API + the test-control endpoints in
// internal/server/lifecycle_testctl_api.go.
//
// SingleNode은 newDedicatedSingleNodeS3Target으로, Cluster4Node는
// newDedicatedCluster4NodeS3Target으로 부트한다 — 두 fixture 모두
// --lifecycle-interval=24h로 lifecycle 서비스를 활성화한다. DM
// sub-test가 versioning을 요구하므로 Cluster4Node 분기는 필수.
func TestLifecycleExpirationE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runLifecycleExpirationCases(t, newDedicatedSingleNodeS3Target(t, []string{"--lifecycle-interval=24h"}))
	})
	t.Run("Cluster4Node", func(t *testing.T) {
		runLifecycleExpirationCases(t, newDedicatedCluster4NodeS3Target(t, nil))
	})
}

func runLifecycleExpirationCases(t *testing.T, tgt s3Target) {
	client := tgt.pickNode(0)
	lc := newLifecycleFixture(t, tgt)

	t.Run("TagFilter", func(t *testing.T) {
		ctx := context.Background()
		bucket := tgt.uniqueBucket(t, "tag")

		for _, key := range []string{"keep", "drop"} {
			_, err := client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucket), Key: aws.String(key), Body: stringReader("body"),
			})
			require.NoError(t, err)
		}
		_, err := client.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{
			Bucket: aws.String(bucket), Key: aws.String("drop"),
			Tagging: &types.Tagging{TagSet: []types.Tag{{Key: aws.String("expire"), Value: aws.String("yes")}}},
		})
		require.NoError(t, err)

		_, err = client.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{
			Bucket: aws.String(bucket),
			LifecycleConfiguration: &types.BucketLifecycleConfiguration{Rules: []types.LifecycleRule{{
				ID:     aws.String("by-tag"),
				Status: types.ExpirationStatusEnabled,
				Filter: &types.LifecycleRuleFilter{
					Tag: &types.Tag{Key: aws.String("expire"), Value: aws.String("yes")},
				},
				Expiration: &types.LifecycleExpiration{Days: aws.Int32(1)},
			}}},
		})
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			_, err := client.GetBucketLifecycleConfiguration(ctx, &s3.GetBucketLifecycleConfigurationInput{
				Bucket: aws.String(bucket),
			})
			return err == nil
		}, 5*time.Second, 100*time.Millisecond, "lifecycle configuration did not become visible")

		lc.AdvanceLifecycleClock(2 * 24 * time.Hour)
		lc.RunLifecycleCycle(ctx)

		require.Eventually(t, func() bool {
			_, err = client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String("drop")})
			return err != nil
		}, 5*time.Second, 100*time.Millisecond, "tag-matched object was not expired")
		require.Error(t, err)
		var apiErr smithy.APIError
		if assert.ErrorAs(t, err, &apiErr) {
			assert.Equal(t, "NotFound", apiErr.ErrorCode())
		}

		_, err = client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String("keep")})
		require.NoError(t, err)
	})

	t.Run("SizeFilter", func(t *testing.T) {
		ctx := context.Background()
		bucket := tgt.uniqueBucket(t, "size")

		sizes := map[string]int{"tiny": 1024, "small": 1 << 20, "big": 100 << 20}
		for key, n := range sizes {
			_, err := client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucket), Key: aws.String(key),
				Body: stringReader(strings.Repeat("a", n)),
			})
			require.NoError(t, err)
		}

		_, err := client.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{
			Bucket: aws.String(bucket),
			LifecycleConfiguration: &types.BucketLifecycleConfiguration{Rules: []types.LifecycleRule{{
				ID:     aws.String("big-only"),
				Status: types.ExpirationStatusEnabled,
				Filter: &types.LifecycleRuleFilter{
					ObjectSizeGreaterThan: aws.Int64(10 << 20),
				},
				Expiration: &types.LifecycleExpiration{Days: aws.Int32(1)},
			}}},
		})
		require.NoError(t, err)

		lc.AdvanceLifecycleClock(2 * 24 * time.Hour)
		lc.RunLifecycleCycle(ctx)

		_, err = client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String("big")})
		require.Error(t, err, "big (100 MiB) must be expired")

		for _, k := range []string{"tiny", "small"} {
			_, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(k)})
			require.NoError(t, err, "%s must remain (below 10 MiB threshold)", k)
		}
	})

	t.Run("AndFilter", func(t *testing.T) {
		ctx := context.Background()
		bucket := tgt.uniqueBucket(t, "and")

		type entry struct {
			key  string
			body string
			tag  bool
		}
		entries := []entry{
			{"logs/match", strings.Repeat("x", 200), true},
			{"logs/small", strings.Repeat("x", 50), true},
			{"logs/notag", strings.Repeat("x", 200), false},
			{"data/big", strings.Repeat("x", 200), true},
			{"data/none", strings.Repeat("x", 50), false},
		}
		for _, e := range entries {
			_, err := client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucket), Key: aws.String(e.key), Body: stringReader(e.body),
			})
			require.NoError(t, err)
			if e.tag {
				_, err := client.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{
					Bucket: aws.String(bucket), Key: aws.String(e.key),
					Tagging: &types.Tagging{TagSet: []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}}},
				})
				require.NoError(t, err)
			}
		}

		_, err := client.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{
			Bucket: aws.String(bucket),
			LifecycleConfiguration: &types.BucketLifecycleConfiguration{Rules: []types.LifecycleRule{{
				ID:     aws.String("and-filter"),
				Status: types.ExpirationStatusEnabled,
				Filter: &types.LifecycleRuleFilter{
					And: &types.LifecycleRuleAndOperator{
						Prefix:                aws.String("logs/"),
						Tags:                  []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
						ObjectSizeGreaterThan: aws.Int64(100),
					},
				},
				Expiration: &types.LifecycleExpiration{Days: aws.Int32(1)},
			}}},
		})
		require.NoError(t, err)

		lc.AdvanceLifecycleClock(2 * 24 * time.Hour)
		lc.RunLifecycleCycle(ctx)

		_, err = client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String("logs/match")})
		require.Error(t, err, "logs/match must be expired (all three criteria match)")

		for _, k := range []string{"logs/small", "logs/notag", "data/big", "data/none"} {
			_, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(k)})
			require.NoError(t, err, "%s must remain", k)
		}
	})

	t.Run("ExpirationDate", func(t *testing.T) {
		ctx := context.Background()
		bucket := tgt.uniqueBucket(t, "date")

		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket), Key: aws.String("k"), Body: stringReader("body"),
		})
		require.NoError(t, err)

		yesterday := time.Now().UTC().Add(-24 * time.Hour).Truncate(24 * time.Hour)
		_, err = client.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{
			Bucket: aws.String(bucket),
			LifecycleConfiguration: &types.BucketLifecycleConfiguration{Rules: []types.LifecycleRule{{
				ID:         aws.String("date-rule"),
				Status:     types.ExpirationStatusEnabled,
				Filter:     &types.LifecycleRuleFilter{Prefix: aws.String("")},
				Expiration: &types.LifecycleExpiration{Date: aws.Time(yesterday)},
			}}},
		})
		require.NoError(t, err)

		lc.RunLifecycleCycle(ctx) // 시계 advance 불필요 — Date가 이미 과거

		_, err = client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String("k")})
		require.Error(t, err, "object must be expired by past Date")
	})

	t.Run("ExpiredObjectDeleteMarker_ChainedReclaim", func(t *testing.T) {
		if tgt.name == "single-dedicated" {
			t.Skip("DM reclaim requires versioning; SingleNode 미지원")
		}
		ctx := context.Background()
		bucket := tgt.uniqueBucket(t, "dm")
		_, err := client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
			Bucket:                  aws.String(bucket),
			VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
		})
		require.NoError(t, err)

		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket), Key: aws.String("k"), Body: stringReader("v1"),
		})
		require.NoError(t, err)
		_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucket), Key: aws.String("k"),
		})
		require.NoError(t, err)

		tru := true
		_, err = client.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{
			Bucket: aws.String(bucket),
			LifecycleConfiguration: &types.BucketLifecycleConfiguration{Rules: []types.LifecycleRule{{
				ID:                          aws.String("dm-and-noncurrent"),
				Status:                      types.ExpirationStatusEnabled,
				Filter:                      &types.LifecycleRuleFilter{Prefix: aws.String("")},
				Expiration:                  &types.LifecycleExpiration{ExpiredObjectDeleteMarker: &tru},
				NoncurrentVersionExpiration: &types.NoncurrentVersionExpiration{NoncurrentDays: aws.Int32(1)},
			}}},
		})
		require.NoError(t, err)

		// Cycle 1: noncurrent v1 expires; DM still present (not yet lone).
		lc.AdvanceLifecycleClock(2 * 24 * time.Hour)
		lc.RunLifecycleCycle(ctx)
		out, err := client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
			Bucket: aws.String(bucket), Prefix: aws.String("k"),
		})
		require.NoError(t, err)
		require.Empty(t, out.Versions, "noncurrent v1 must be reclaimed")
		require.Len(t, out.DeleteMarkers, 1, "DM still present (not yet lone-reclaimed)")

		// Cycle 2: lone DM reclaimed.
		lc.RunLifecycleCycle(ctx)
		out, err = client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
			Bucket: aws.String(bucket), Prefix: aws.String("k"),
		})
		require.NoError(t, err)
		require.Empty(t, out.Versions)
		require.Empty(t, out.DeleteMarkers, "lone DM must be reclaimed on next cycle")
	})

	t.Run("AbortIncompleteMultipartUpload", func(t *testing.T) {
		ctx := context.Background()
		bucket := tgt.uniqueBucket(t, "mpu")

		// Cluster4Node requires the multipart_listing_v1 capability to be
		// acknowledged before CreateMultipartUpload succeeds on a fresh bucket.
		if tgt.isCluster {
			waitForMultipartListingCreate(t, ctx, client, bucket, "k", 120*time.Second)
		}

		created, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucket), Key: aws.String("k"),
		})
		require.NoError(t, err)
		// Do NOT complete — leave the MPU abandoned.

		_, err = client.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{
			Bucket: aws.String(bucket),
			LifecycleConfiguration: &types.BucketLifecycleConfiguration{Rules: []types.LifecycleRule{{
				ID:                             aws.String("abort-mpu"),
				Status:                         types.ExpirationStatusEnabled,
				Filter:                         &types.LifecycleRuleFilter{Prefix: aws.String("")},
				AbortIncompleteMultipartUpload: &types.AbortIncompleteMultipartUpload{DaysAfterInitiation: aws.Int32(1)},
			}}},
		})
		require.NoError(t, err)

		lc.AdvanceLifecycleClock(2 * 24 * time.Hour)
		lc.RunLifecycleCycle(ctx)

		out, err := client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: aws.String(bucket)})
		require.NoError(t, err)
		for _, u := range out.Uploads {
			require.NotEqual(t, *created.UploadId, *u.UploadId, "abandoned MPU must be aborted")
		}
	})
}
