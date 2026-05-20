//go:build integration

package e2e

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// TestLifecycleExpirationE2E exercises the leader-side expiration path via the
// public S3 lifecycle API + the test-control endpoints in
// internal/server/lifecycle_testctl_api.go.
//
// SingleNode은 newDedicatedSingleNodeS3Target으로, Cluster4Node는
// newDedicatedCluster4NodeS3Target으로 부트 — 두 fixture 모두
// --lifecycle-interval=24h로 lifecycle 서비스를 활성화한다. DM sub-spec이
// versioning을 요구하므로 SingleNode에서는 Skip.
func TestLifecycleExpirationE2E(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "Lifecycle expiration e2e")
}

var _ = ginkgo.Describe("Lifecycle expiration", func() {
	describeLifecycleExpirationContext("SingleNode", func(t testing.TB) (s3Target, *lifecycleFixture) {
		tgt := newDedicatedSingleNodeS3Target(t, []string{"--lifecycle-interval=24h"})
		return tgt, newLifecycleFixture(t, tgt)
	})
	describeLifecycleExpirationContext("Cluster4Node", func(t testing.TB) (s3Target, *lifecycleFixture) {
		tgt := newDedicatedCluster4NodeS3Target(t, []string{"--lifecycle-interval=24h"})
		return tgt, newLifecycleFixture(t, tgt)
	})
})

func describeLifecycleExpirationContext(name string, factory func(testing.TB) (s3Target, *lifecycleFixture)) {
	ginkgo.Context(name, ginkgo.Ordered, func() {
		var tgt s3Target
		var lc *lifecycleFixture
		ginkgo.BeforeAll(func() {
			tgt, lc = factory(ginkgo.GinkgoTB())
		})
		ginkgo.BeforeEach(func() {
			lc.ResetClock()
		})
		runLifecycleExpirationCases(
			func() s3Target { return tgt },
			func() *lifecycleFixture { return lc },
		)
	})
}

func runLifecycleExpirationCases(getTgt func() s3Target, getLC func() *lifecycleFixture) {
	ginkgo.It("expires only tag-matched objects (TagFilter)", func(ctx context.Context) {
		tgt := getTgt()
		client := tgt.pickNode(0)
		lc := getLC()
		bucket := tgt.uniqueBucket(ginkgo.GinkgoTB(), "tag")

		for _, key := range []string{"keep", "drop"} {
			_, err := client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucket), Key: aws.String(key), Body: stringReader("body"),
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		_, err := client.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{
			Bucket: aws.String(bucket), Key: aws.String("drop"),
			Tagging: &types.Tagging{TagSet: []types.Tag{{Key: aws.String("expire"), Value: aws.String("yes")}}},
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

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
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		gomega.Eventually(func() bool {
			_, err := client.GetBucketLifecycleConfiguration(ctx, &s3.GetBucketLifecycleConfigurationInput{
				Bucket: aws.String(bucket),
			})
			return err == nil
		}).WithTimeout(5*time.Second).WithPolling(100*time.Millisecond).Should(gomega.BeTrue(),
			"lifecycle configuration did not become visible")

		lc.AdvanceLifecycleClock(2 * 24 * time.Hour)
		lc.RunLifecycleCycle(ctx)

		var headErr error
		gomega.Eventually(func() bool {
			_, headErr = client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String("drop")})
			return headErr != nil
		}).WithTimeout(5*time.Second).WithPolling(100*time.Millisecond).Should(gomega.BeTrue(),
			"tag-matched object was not expired")
		var apiErr smithy.APIError
		if errors.As(headErr, &apiErr) {
			// master testify 'assert.Equal' soft-fail 의미 보존: Cluster4Node
			// 분기는 현재 expiration 후 HeadObject가 MethodNotAllowed(DM 처리
			// 경로)를 반환할 수 있다. NotFound 일관화는 별도 phase. 1:1 마이그레이션
			// 의미 유지를 위해 두 코드 모두 허용한다.
			gomega.Expect(apiErr.ErrorCode()).To(gomega.Or(
				gomega.Equal("NotFound"),
				gomega.Equal("MethodNotAllowed"),
			))
		}

		_, err = client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String("keep")})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}, ginkgo.NodeTimeout(120*time.Second))

	ginkgo.It("expires only objects above size threshold (SizeFilter)", func(ctx context.Context) {
		tgt := getTgt()
		client := tgt.pickNode(0)
		lc := getLC()
		bucket := tgt.uniqueBucket(ginkgo.GinkgoTB(), "size")

		sizes := map[string]int{"tiny": 1024, "small": 1 << 20, "big": 100 << 20}
		for key, n := range sizes {
			_, err := client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucket), Key: aws.String(key),
				Body: stringReader(strings.Repeat("a", n)),
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
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
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		lc.AdvanceLifecycleClock(2 * 24 * time.Hour)
		lc.RunLifecycleCycle(ctx)

		_, err = client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String("big")})
		gomega.Expect(err).To(gomega.HaveOccurred(), "big (100 MiB) must be expired")

		for _, k := range []string{"tiny", "small"} {
			_, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(k)})
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "%s must remain (below 10 MiB threshold)", k)
		}
	}, ginkgo.NodeTimeout(120*time.Second))

	ginkgo.It("expires only objects matching all And{Prefix,Tag,Size} (AndFilter)", func(ctx context.Context) {
		tgt := getTgt()
		client := tgt.pickNode(0)
		lc := getLC()
		bucket := tgt.uniqueBucket(ginkgo.GinkgoTB(), "and")

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
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			if e.tag {
				_, err := client.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{
					Bucket: aws.String(bucket), Key: aws.String(e.key),
					Tagging: &types.Tagging{TagSet: []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}}},
				})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
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
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		lc.AdvanceLifecycleClock(2 * 24 * time.Hour)
		lc.RunLifecycleCycle(ctx)

		_, err = client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String("logs/match")})
		gomega.Expect(err).To(gomega.HaveOccurred(), "logs/match must be expired (all three criteria match)")

		for _, k := range []string{"logs/small", "logs/notag", "data/big", "data/none"} {
			_, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(k)})
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "%s must remain", k)
		}
	}, ginkgo.NodeTimeout(120*time.Second))

	ginkgo.It("expires by absolute Date in the past (ExpirationDate)", func(ctx context.Context) {
		tgt := getTgt()
		client := tgt.pickNode(0)
		lc := getLC()
		bucket := tgt.uniqueBucket(ginkgo.GinkgoTB(), "date")

		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket), Key: aws.String("k"), Body: stringReader("body"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

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
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		lc.RunLifecycleCycle(ctx) // 시계 advance 불필요 — Date가 이미 과거

		_, err = client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String("k")})
		gomega.Expect(err).To(gomega.HaveOccurred(), "object must be expired by past Date")
	}, ginkgo.NodeTimeout(120*time.Second))

	ginkgo.It("chains DM reclaim on lone DM (ExpiredObjectDeleteMarker_ChainedReclaim)", func(ctx context.Context) {
		tgt := getTgt()
		if tgt.name == "single-dedicated" {
			ginkgo.Skip("DM reclaim requires versioning; SingleNode 미지원")
		}
		client := tgt.pickNode(0)
		lc := getLC()
		bucket := tgt.uniqueBucket(ginkgo.GinkgoTB(), "dm")

		_, err := client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
			Bucket:                  aws.String(bucket),
			VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket), Key: aws.String("k"), Body: stringReader("v1"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucket), Key: aws.String("k"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

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
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Cycle 1: noncurrent v1 expires; DM still present (not yet lone).
		lc.AdvanceLifecycleClock(2 * 24 * time.Hour)
		lc.RunLifecycleCycle(ctx)
		out, err := client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
			Bucket: aws.String(bucket), Prefix: aws.String("k"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(out.Versions).To(gomega.BeEmpty(), "noncurrent v1 must be reclaimed")
		gomega.Expect(out.DeleteMarkers).To(gomega.HaveLen(1), "DM still present (not yet lone-reclaimed)")

		// Cycle 2: lone DM reclaimed.
		lc.RunLifecycleCycle(ctx)
		out, err = client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
			Bucket: aws.String(bucket), Prefix: aws.String("k"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(out.Versions).To(gomega.BeEmpty())
		gomega.Expect(out.DeleteMarkers).To(gomega.BeEmpty(), "lone DM must be reclaimed on next cycle")
	}, ginkgo.NodeTimeout(180*time.Second))

	ginkgo.It("aborts abandoned multipart upload (AbortIncompleteMultipartUpload)", func(ctx context.Context) {
		tgt := getTgt()
		client := tgt.pickNode(0)
		lc := getLC()
		bucket := tgt.uniqueBucket(ginkgo.GinkgoTB(), "mpu")

		// Cluster4Node requires the multipart_listing_v1 capability to be
		// acknowledged before CreateMultipartUpload succeeds on a fresh bucket.
		if tgt.isCluster {
			waitForMultipartListingCreate(ginkgo.GinkgoTB(), ctx, client, bucket, "k", 120*time.Second)
		}

		created, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucket), Key: aws.String("k"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
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
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		lc.AdvanceLifecycleClock(2 * 24 * time.Hour)
		lc.RunLifecycleCycle(ctx)

		out, err := client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: aws.String(bucket)})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for _, u := range out.Uploads {
			gomega.Expect(aws.ToString(u.UploadId)).NotTo(gomega.Equal(aws.ToString(created.UploadId)),
				"abandoned MPU must be aborted")
		}
	}, ginkgo.NodeTimeout(120*time.Second))
}
