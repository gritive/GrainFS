//go:build integration

package e2e

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Lifecycle config", func() {
	describeLifecycleConfigContext("SingleNode", func() (s3Target, *lifecycleFixture) {
		tb := ginkgo.GinkgoTB()
		tgt := newDedicatedSingleNodeS3Target(tb, []string{"--lifecycle-interval=24h"})
		return tgt, newLifecycleFixture(tb, tgt)
	})
	describeLifecycleConfigContext("Cluster4Node", func() (s3Target, *lifecycleFixture) {
		tb := ginkgo.GinkgoTB()
		tgt := newDedicatedCluster4NodeS3Target(tb, []string{"--lifecycle-interval=24h"})
		return tgt, newLifecycleFixture(tb, tgt)
	})
})

// describeLifecycleConfigContext registers an Ordered Ginkgo Context whose
// fixture (target + lifecycleFixture) is built once in BeforeAll and shared
// across all specs. BeforeEach calls lc.ResetClock() so the server-side
// lifecycle worker clock starts at real-now for each spec, preventing
// cumulative drift across specs (every prior AdvanceLifecycleClock call
// would otherwise leak into the next spec's PutObject/expiration window).
//
// factory는 인자 없는 ginkgo-native closure — BeforeAll 안에서만 호출되며
// 내부에서 ginkgo.GinkgoTB()를 직접 받는다. helper들(newDedicated*S3Target,
// newLifecycleFixture)이 testing.TB를 요구하므로 GinkgoTB 호출은 factory가 담당.
func describeLifecycleConfigContext(name string, factory func() (s3Target, *lifecycleFixture)) {
	ginkgo.Context(name, ginkgo.Ordered, func() {
		var tgt s3Target
		var lc *lifecycleFixture
		ginkgo.BeforeAll(func() {
			tgt, lc = factory()
		})
		ginkgo.BeforeEach(func() {
			lc.ResetClock()
		})
		runLifecycleConfigCases(
			func() s3Target { return tgt },
			func() *lifecycleFixture { return lc },
		)
	})
}

// runLifecycleConfigCases registers all It-specs against whichever fixture is
// set up by the surrounding Context's BeforeEach. Mirrors the t.Run pattern's
// runXxxCases(t, tgt) helper. The getter closures defer fixture access to the
// It body so each It sees the freshly-rebooted fixture from BeforeEach.
func runLifecycleConfigCases(
	getTgt func() s3Target,
	getLC func() *lifecycleFixture,
) {
	ginkgo.It("round-trips Put → Get (PutGetRoundTrip)", func(ctx context.Context) {
		tgt := getTgt()
		client := tgt.pickNode(0)
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
	}, ginkgo.NodeTimeout(120*time.Second))

	ginkgo.It("returns NoSuchLifecycleConfiguration after Delete (DeleteThenGet404)", func(ctx context.Context) {
		tgt := getTgt()
		client := tgt.pickNode(0)
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
	}, ginkgo.NodeTimeout(120*time.Second))

	ginkgo.It("ignores Disabled rule during scan (DisabledRuleIgnored)", func(ctx context.Context) {
		tgt := getTgt()
		client := tgt.pickNode(0)
		lc := getLC()
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
	}, ginkgo.NodeTimeout(120*time.Second))

	ginkgo.It("scans empty bucket without panic (EmptyBucketScanNoPanic)", func(ctx context.Context) {
		tgt := getTgt()
		client := tgt.pickNode(0)
		lc := getLC()
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
	}, ginkgo.NodeTimeout(120*time.Second))

	ginkgo.It("reclaims noncurrent versions standalone without DM (NoncurrentVersionExpirationStandalone)", func(ctx context.Context) {
		tgt := getTgt()
		if tgt.name == "single-dedicated" {
			ginkgo.Skip("noncurrent version reclaim requires versioning; LocalBackend SingleNode 미지원")
		}
		client := tgt.pickNode(0)
		lc := getLC()
		bucket := tgt.uniqueBucket(ginkgo.GinkgoTB(), "lcncv")

		// Enable versioning
		_, err := client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
			Bucket:                  aws.String(bucket),
			VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Put v1, then v2 (v1 becomes noncurrent)
		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket), Key: aws.String("k"), Body: stringReader("v1"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket), Key: aws.String("k"), Body: stringReader("v2"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// NoncurrentVersionExpiration only — NO Expiration, NO ExpiredObjectDeleteMarker
		_, err = client.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{
			Bucket: aws.String(bucket),
			LifecycleConfiguration: &types.BucketLifecycleConfiguration{Rules: []types.LifecycleRule{{
				ID:     aws.String("ncv-only"),
				Status: types.ExpirationStatusEnabled,
				Filter: &types.LifecycleRuleFilter{Prefix: aws.String("")},
				NoncurrentVersionExpiration: &types.NoncurrentVersionExpiration{
					NoncurrentDays: aws.Int32(1),
				},
			}}},
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		lc.AdvanceLifecycleClock(2 * 24 * time.Hour)
		lc.RunLifecycleCycle(ctx)

		// v1 (noncurrent) must be reclaimed; v2 (current) must remain; no DM
		out, err := client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
			Bucket: aws.String(bucket), Prefix: aws.String("k"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(out.Versions).To(gomega.HaveLen(1), "only current v2 must remain")
		gomega.Expect(aws.ToBool(out.Versions[0].IsLatest)).To(gomega.BeTrue())
		gomega.Expect(out.DeleteMarkers).To(gomega.BeEmpty(), "no DM expected (standalone NoncurrentVersionExpiration)")

		// HEAD must still return v2
		head, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket), Key: aws.String("k"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "current v2 must still be reachable via HeadObject")
		gomega.Expect(head.ContentLength).NotTo(gomega.BeNil())
	}, ginkgo.NodeTimeout(180*time.Second))

	// Step 1 finding: applyRulesToGroup iterates rules slice in declaration order and applies
	// all matching rules sequentially (no specificity ranking). Multiple matching rules all
	// evaluate; effects accumulate. The narrower prefix rule with shorter Days wins because
	// its expiration trigger fires sooner.
	ginkgo.It("applies all matching rules sequentially — narrower wins via shorter Days (MultipleRulesPriority)", func(ctx context.Context) {
		tgt := getTgt()
		client := tgt.pickNode(0)
		lc := getLC()
		bucket := tgt.uniqueBucket(ginkgo.GinkgoTB(), "lcmr")

		// Two objects: logs/critical matches both "logs/" (broad) and "logs/critical" (narrow),
		// logs/normal matches only "logs/" (broad).
		for _, key := range []string{"logs/critical", "logs/normal"} {
			_, err := client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucket), Key: aws.String(key), Body: stringReader("body"),
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		_, err := client.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{
			Bucket: aws.String(bucket),
			LifecycleConfiguration: &types.BucketLifecycleConfiguration{Rules: []types.LifecycleRule{
				{
					ID:         aws.String("broad-logs"),
					Status:     types.ExpirationStatusEnabled,
					Filter:     &types.LifecycleRuleFilter{Prefix: aws.String("logs/")},
					Expiration: &types.LifecycleExpiration{Days: aws.Int32(30)},
				},
				{
					ID:         aws.String("narrow-critical"),
					Status:     types.ExpirationStatusEnabled,
					Filter:     &types.LifecycleRuleFilter{Prefix: aws.String("logs/critical")},
					Expiration: &types.LifecycleExpiration{Days: aws.Int32(1)},
				},
			}},
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Advance 2 days — narrow rule triggers logs/critical; broad rule (30d) doesn't yet
		lc.AdvanceLifecycleClock(2 * 24 * time.Hour)
		lc.RunLifecycleCycle(ctx)

		_, err = client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket), Key: aws.String("logs/critical"),
		})
		gomega.Expect(err).To(gomega.HaveOccurred(), "logs/critical must be expired by narrow rule (1d)")

		_, err = client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket), Key: aws.String("logs/normal"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "logs/normal must remain — broad rule (30d) not yet triggered")
	}, ginkgo.NodeTimeout(120*time.Second))
}
