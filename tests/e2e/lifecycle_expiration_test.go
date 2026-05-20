package e2e

import (
	"context"
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

		lc.AdvanceLifecycleClock(2 * 24 * time.Hour)
		lc.RunLifecycleCycle(ctx)

		_, err = client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String("drop")})
		require.Error(t, err)
		var apiErr smithy.APIError
		if assert.ErrorAs(t, err, &apiErr) {
			assert.Equal(t, "NotFound", apiErr.ErrorCode())
		}

		_, err = client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String("keep")})
		require.NoError(t, err)
	})
}
