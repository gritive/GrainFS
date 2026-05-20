//go:build integration

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

// TestLifecycleWorker_RunsAfterBucketCreate guards against R4 (Phase 1
// followup): lifecycle Worker.runCycle must actually process freshly
// created buckets — not no-op silently. Uses ≥65 KiB body to bypass
// PackedBackend (R3 is fixed by sibling tasks in this phase).
func TestLifecycleWorker_RunsAfterBucketCreate(t *testing.T) {
	tgt := newDedicatedSingleNodeS3Target(t, []string{"--lifecycle-interval=24h"})
	client := tgt.pickNode(0)
	lc := newLifecycleFixture(t, tgt)
	ctx := context.Background()

	bucket := tgt.uniqueBucket(t, "r4")

	// Body ≥65 KiB to bypass PackedBackend so this test isolates R4
	// (lifecycle worker not running) from R3 (worker can't see packed).
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
	require.NoError(t, err, "PutBucketLifecycleConfiguration must succeed (R2 fix verifier)")

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
}
