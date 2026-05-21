//go:build integration

// Lifecycle worker e2e (Ginkgo v2 patterned).
//
// SingleNode + Cluster4Node 듀얼 분기. Cases는 worker.runCycle 자체의 행동을
// 검증 (R4 family). Phase 1 followup에서 추가됨, Phase 2에서 Ginkgo 마이그레이션.
package e2e

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Lifecycle worker", func() {
	describeLifecycleWorkerContext("SingleNode", func() (s3Target, *lifecycleFixture) {
		tb := ginkgo.GinkgoTB()
		tgt := newDedicatedSingleNodeS3Target(tb, []string{"--lifecycle-interval=24h"})
		return tgt, newLifecycleFixture(tb, tgt)
	})
	describeLifecycleWorkerContext("Cluster4Node", func() (s3Target, *lifecycleFixture) {
		tb := ginkgo.GinkgoTB()
		tgt := newDedicatedCluster4NodeS3Target(tb, []string{"--lifecycle-interval=24h"})
		return tgt, newLifecycleFixture(tb, tgt)
	})
})

func describeLifecycleWorkerContext(name string, factory func() (s3Target, *lifecycleFixture)) {
	ginkgo.Context(name, ginkgo.Ordered, func() {
		var tgt s3Target
		var lc *lifecycleFixture
		ginkgo.BeforeAll(func() {
			tgt, lc = factory()
		})
		ginkgo.BeforeEach(func() {
			lc.ResetClock()
		})
		runLifecycleWorkerCases(
			func() s3Target { return tgt },
			func() *lifecycleFixture { return lc },
		)
	})
}

func runLifecycleWorkerCases(getTgt func() s3Target, getLC func() *lifecycleFixture) {
	// RunsAfterBucketCreate guards R4: worker.runCycle must actually process
	// freshly created buckets. Body ≥65 KiB to bypass PackedBackend so this
	// case isolates the ClusterCoordinator multi-group scan path from R3
	// (PackedBackend.ScanObjectsGrouped) and R5 (packed delete cleanup).
	ginkgo.It("runs lifecycle worker right after bucket create (RunsAfterBucketCreate)", func(ctx context.Context) {
		tgt := getTgt()
		client := tgt.pickNode(0)
		lc := getLC()
		bucket := tgt.uniqueBucket(ginkgo.GinkgoTB(), "r4")

		body := strings.Repeat("a", 70*1024)

		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("expire-me"),
			Body:   stringReader(body),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "PutObject must succeed")

		_, err = client.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{
			Bucket: aws.String(bucket),
			LifecycleConfiguration: &types.BucketLifecycleConfiguration{Rules: []types.LifecycleRule{{
				ID:         aws.String("r4-guard"),
				Status:     types.ExpirationStatusEnabled,
				Filter:     &types.LifecycleRuleFilter{Prefix: aws.String("")},
				Expiration: &types.LifecycleExpiration{Days: aws.Int32(1)},
			}}},
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "PutBucketLifecycleConfiguration must succeed")

		lc.AdvanceLifecycleClock(2 * 24 * time.Hour)
		lc.RunLifecycleCycle(ctx)

		_, err = client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket), Key: aws.String("expire-me"),
		})
		gomega.Expect(err).To(gomega.HaveOccurred(), "lifecycle worker must expire the object after cycle (R4 regression guard)")
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			// master testify 'assert.Equal' soft-fail 의미 보존:
			// Cluster4Node 분기는 NotFound 또는 MethodNotAllowed 모두 가능.
			gomega.Expect(apiErr.ErrorCode()).To(gomega.Or(
				gomega.Equal("NotFound"),
				gomega.Equal("MethodNotAllowed"),
			))
		}
	}, ginkgo.NodeTimeout(120*time.Second))
}
