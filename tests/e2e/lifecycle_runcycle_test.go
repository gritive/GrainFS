// Lifecycle worker e2e (target table-driven).
//
// SingleNode + Cluster4Node 듀얼 분기. Cases는 worker.runCycle 자체의 행동을
// 검증 (R4 family). Phase 1 followup에서 추가됨.
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

func TestLifecycleWorkerE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runLifecycleWorkerCases(t, newDedicatedSingleNodeS3Target(t, []string{"--lifecycle-interval=24h"}))
	})

	t.Run("Cluster4Node", func(t *testing.T) {
		runLifecycleWorkerCases(t, newDedicatedCluster4NodeS3Target(t, []string{"--lifecycle-interval=24h"}))
	})
}

func runLifecycleWorkerCases(t *testing.T, tgt s3Target) {
	client := tgt.pickNode(0)
	lc := newLifecycleFixture(t, tgt)

	// RunsAfterBucketCreate guards R4: worker.runCycle must actually process
	// freshly created buckets. Body ≥65 KiB to bypass PackedBackend so this
	// case isolates the ClusterCoordinator multi-group scan path from R3
	// (PackedBackend.ScanObjectsGrouped) and R5 (packed delete cleanup).
	t.Run("RunsAfterBucketCreate", func(t *testing.T) {
		ctx := context.Background()
		bucket := tgt.uniqueBucket(t, "r4")

		body := strings.Repeat("a", 70*1024)

		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("expire-me"),
			Body:   stringReader(body),
		})
		require.NoError(t, err, "PutObject must succeed")

		_, err = client.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{
			Bucket: aws.String(bucket),
			LifecycleConfiguration: &types.BucketLifecycleConfiguration{Rules: []types.LifecycleRule{{
				ID:         aws.String("r4-guard"),
				Status:     types.ExpirationStatusEnabled,
				Filter:     &types.LifecycleRuleFilter{Prefix: aws.String("")},
				Expiration: &types.LifecycleExpiration{Days: aws.Int32(1)},
			}}},
		})
		require.NoError(t, err, "PutBucketLifecycleConfiguration must succeed")

		lc.AdvanceLifecycleClock(2 * 24 * time.Hour)
		lc.RunLifecycleCycle(ctx)

		_, err = client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket), Key: aws.String("expire-me"),
		})
		require.Error(t, err, "lifecycle worker must expire the object after cycle (R4 regression guard)")
		var apiErr smithy.APIError
		if assert.ErrorAs(t, err, &apiErr) {
			assert.Equal(t, "NotFound", apiErr.ErrorCode())
		}
	})
}
