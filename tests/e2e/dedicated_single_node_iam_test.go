//go:build integration

package e2e

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/onsi/ginkgo/v2"
	"github.com/stretchr/testify/require"
)

// TestDedicatedSingleNode_AdminGrant_Regression guards against R2
// (Phase 2 unblock fixes): the admin SA bootstrapped by
// newDedicatedSingleNodeS3Target must have grants for both basic S3 ops
// (PutObject, HeadObject) and management ops
// (PutBucketLifecycleConfiguration). Manifests at HEAD as 403 AccessDenied
// on PutBucketLifecycleConfiguration with "IAM grant denies this action".
var _ = ginkgo.Describe("Dedicated single-node IAM", func() {
	ginkgo.It("grants the admin service account data and lifecycle permissions", func() {
		t := ginkgo.GinkgoTB()
		// --lifecycle-interval=24h boots the lifecycle service so
		// PutBucketLifecycleConfiguration reaches the handler instead of 501
		// NotImplemented. The IAM grant — not the service availability — is what
		// this regression guards.
		tgt := newDedicatedSingleNodeS3Target(t, []string{"--lifecycle-interval=24h"})
		client := tgt.pickNode(0)
		ctx := context.Background()

		bucket := tgt.uniqueBucket(t, "grant")

		// Sub-case 1: basic data path — PutObject + HeadObject must succeed.
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("k"),
			Body:   stringReader("body"),
		})
		require.NoError(t, err, "PutObject must succeed under dedicated single-node admin SA")

		_, err = client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("k"),
		})
		require.NoError(t, err, "HeadObject must succeed under dedicated single-node admin SA")

		// Sub-case 2: management API — PutBucketLifecycleConfiguration must
		// succeed (R2 primary manifestation at HEAD: 403 AccessDenied).
		_, err = client.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{
			Bucket: aws.String(bucket),
			LifecycleConfiguration: &types.BucketLifecycleConfiguration{
				Rules: []types.LifecycleRule{{
					ID:         aws.String("r2-guard"),
					Status:     types.ExpirationStatusEnabled,
					Filter:     &types.LifecycleRuleFilter{Prefix: aws.String("")},
					Expiration: &types.LifecycleExpiration{Days: aws.Int32(1)},
				}},
			},
		})
		require.NoError(t, err, "PutBucketLifecycleConfiguration must succeed (R2 regression guard)")
	})
})
