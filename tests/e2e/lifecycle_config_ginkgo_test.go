//go:build integration

package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

func TestLifecycleConfigGinkgo(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "Lifecycle Config e2e (PoC)")
}

var _ = ginkgo.Describe("Lifecycle config", func() {
	ginkgo.Context("SingleNode", func() {
		var tgt s3Target
		var lc *lifecycleFixture
		ginkgo.BeforeEach(func() {
			tgt = newDedicatedSingleNodeS3Target(ginkgo.GinkgoTB(), []string{"--lifecycle-interval=24h"})
			lc = newLifecycleFixture(ginkgo.GinkgoTB(), tgt)
		})
		runLifecycleConfigCases(
			func() s3Target { return tgt },
			func() *lifecycleFixture { return lc },
		)
	})

	ginkgo.Context("Cluster4Node", func() {
		var tgt s3Target
		var lc *lifecycleFixture
		ginkgo.BeforeEach(func() {
			tgt = newDedicatedCluster4NodeS3Target(ginkgo.GinkgoTB(), nil)
			lc = newLifecycleFixture(ginkgo.GinkgoTB(), tgt)
		})
		runLifecycleConfigCases(
			func() s3Target { return tgt },
			func() *lifecycleFixture { return lc },
		)
	})
})

// runLifecycleConfigCases registers all It-specs against whichever fixture is
// set up by the surrounding Context's BeforeEach. Mirrors the t.Run pattern's
// runXxxCases(t, tgt) helper. The getter closures defer fixture access to the
// It body so each It sees the freshly-rebooted fixture from BeforeEach.
func runLifecycleConfigCases(
	getTgt func() s3Target,
	getLC func() *lifecycleFixture,
) {
	ginkgo.It("round-trips Put → Get (PutGetRoundTrip)", func() {
		tgt := getTgt()
		client := tgt.pickNode(0)
		ctx := context.Background()
		bucket := tgt.uniqueBucket(ginkgo.GinkgoTB(), "lcrt")

		// Put two rules with distinct semantics so round-trip catches
		// per-field serialization issues, not just rule count.
		putIn := &s3.PutBucketLifecycleConfigurationInput{
			Bucket: aws.String(bucket),
			LifecycleConfiguration: &types.BucketLifecycleConfiguration{Rules: []types.LifecycleRule{
				{
					ID:         aws.String("expire-30d"),
					Status:     types.ExpirationStatusEnabled,
					Filter:     &types.LifecycleRuleFilter{Prefix: aws.String("logs/")},
					Expiration: &types.LifecycleExpiration{Days: aws.Int32(30)},
				},
				{
					ID:     aws.String("noncurrent-7d"),
					Status: types.ExpirationStatusEnabled,
					Filter: &types.LifecycleRuleFilter{Prefix: aws.String("")},
					NoncurrentVersionExpiration: &types.NoncurrentVersionExpiration{
						NoncurrentDays: aws.Int32(7),
					},
				},
			}},
		}
		_, err := client.PutBucketLifecycleConfiguration(ctx, putIn)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "PutBucketLifecycleConfiguration must succeed")

		got, err := client.GetBucketLifecycleConfiguration(ctx, &s3.GetBucketLifecycleConfigurationInput{
			Bucket: aws.String(bucket),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "GetBucketLifecycleConfiguration must succeed")

		gomega.Expect(got.Rules).To(gomega.HaveLen(2))

		// Map by ID for order-insensitive assertion.
		byID := make(map[string]types.LifecycleRule, len(got.Rules))
		for _, r := range got.Rules {
			byID[aws.ToString(r.ID)] = r
		}

		r1, ok := byID["expire-30d"]
		gomega.Expect(ok).To(gomega.BeTrue(), "rule 'expire-30d' must echo back")
		gomega.Expect(r1.Status).To(gomega.Equal(types.ExpirationStatusEnabled))
		gomega.Expect(r1.Filter).NotTo(gomega.BeNil())
		gomega.Expect(aws.ToString(r1.Filter.Prefix)).To(gomega.Equal("logs/"))
		gomega.Expect(r1.Expiration).NotTo(gomega.BeNil())
		gomega.Expect(aws.ToInt32(r1.Expiration.Days)).To(gomega.Equal(int32(30)))

		r2, ok := byID["noncurrent-7d"]
		gomega.Expect(ok).To(gomega.BeTrue(), "rule 'noncurrent-7d' must echo back")
		gomega.Expect(r2.NoncurrentVersionExpiration).NotTo(gomega.BeNil())
		gomega.Expect(aws.ToInt32(r2.NoncurrentVersionExpiration.NoncurrentDays)).To(gomega.Equal(int32(7)))
	})

	ginkgo.It("returns NoSuchLifecycleConfiguration after Delete (DeleteThenGet404)", func() {
		tgt := getTgt()
		client := tgt.pickNode(0)
		ctx := context.Background()
		bucket := tgt.uniqueBucket(ginkgo.GinkgoTB(), "lcdel")

		// Put a minimal config first
		_, err := client.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{
			Bucket: aws.String(bucket),
			LifecycleConfiguration: &types.BucketLifecycleConfiguration{Rules: []types.LifecycleRule{{
				ID:         aws.String("any"),
				Status:     types.ExpirationStatusEnabled,
				Filter:     &types.LifecycleRuleFilter{Prefix: aws.String("")},
				Expiration: &types.LifecycleExpiration{Days: aws.Int32(1)},
			}}},
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Delete it
		_, err = client.DeleteBucketLifecycle(ctx, &s3.DeleteBucketLifecycleInput{Bucket: aws.String(bucket)})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "DeleteBucketLifecycle must succeed")

		// Get must now return NoSuchLifecycleConfiguration
		_, err = client.GetBucketLifecycleConfiguration(ctx, &s3.GetBucketLifecycleConfigurationInput{
			Bucket: aws.String(bucket),
		})
		gomega.Expect(err).To(gomega.HaveOccurred(), "Get must error after Delete")
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("NoSuchLifecycleConfiguration"),
			"error must be NoSuchLifecycleConfiguration code: %v", err)
	})

	ginkgo.It("ignores Disabled rule during scan (DisabledRuleIgnored)", func() {
		tgt := getTgt()
		client := tgt.pickNode(0)
		lc := getLC()
		ctx := context.Background()
		bucket := tgt.uniqueBucket(ginkgo.GinkgoTB(), "lcdis")

		// Put an object that would be expired IF the rule were Enabled
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket), Key: aws.String("survive"),
			Body: stringReader("body"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Rule with Status: Disabled — should be ignored entirely
		_, err = client.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{
			Bucket: aws.String(bucket),
			LifecycleConfiguration: &types.BucketLifecycleConfiguration{Rules: []types.LifecycleRule{{
				ID:         aws.String("disabled-rule"),
				Status:     types.ExpirationStatusDisabled,
				Filter:     &types.LifecycleRuleFilter{Prefix: aws.String("")},
				Expiration: &types.LifecycleExpiration{Days: aws.Int32(1)},
			}}},
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Advance clock + run cycle
		lc.AdvanceLifecycleClock(2 * 24 * time.Hour)
		lc.RunLifecycleCycle(ctx)

		// Object must still be alive
		_, err = client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket), Key: aws.String("survive"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			"object must remain — Disabled rule must not trigger expiration")
	})

	ginkgo.It("scans empty bucket without panic (EmptyBucketScanNoPanic)", func() {
		tgt := getTgt()
		client := tgt.pickNode(0)
		lc := getLC()
		ctx := context.Background()
		bucket := tgt.uniqueBucket(ginkgo.GinkgoTB(), "lcempty")

		// No objects — apply lifecycle config to empty bucket
		_, err := client.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{
			Bucket: aws.String(bucket),
			LifecycleConfiguration: &types.BucketLifecycleConfiguration{Rules: []types.LifecycleRule{{
				ID:         aws.String("any"),
				Status:     types.ExpirationStatusEnabled,
				Filter:     &types.LifecycleRuleFilter{Prefix: aws.String("")},
				Expiration: &types.LifecycleExpiration{Days: aws.Int32(1)},
			}}},
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		lc.AdvanceLifecycleClock(2 * 24 * time.Hour)

		// Cycle must complete without panic / error
		gomega.Expect(func() {
			lc.RunLifecycleCycle(ctx)
		}).NotTo(gomega.Panic(), "RunLifecycleCycle must not panic on empty bucket")

		// Bucket should still be there + listable
		out, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "bucket must still exist after empty-scan cycle")
		gomega.Expect(out.Contents).To(gomega.BeEmpty(), "bucket must still be empty")
	})
}
