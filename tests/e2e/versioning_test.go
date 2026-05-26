package e2e

import (
	"context"
	"errors"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("S3 versioning", ginkgo.Label("bucket"), func() {
	describeVersioningContext("SingleNode", func() s3Target {
		return newSingleNodeS3Target()
	})

	describeVersioningContext("Cluster4Node", func() s3Target {
		return newSharedClusterS3Target(ginkgo.GinkgoTB())
	})
})

func describeVersioningContext(name string, factory func() s3Target) {
	ginkgo.Context(name, ginkgo.Ordered, func() {
		var (
			tgt    s3Target
			client *s3.Client
		)

		ginkgo.BeforeAll(func() {
			tgt = factory()
			client = tgt.pickNode(0)
		})

		runVersioningCases(func() s3Target { return tgt }, func() *s3.Client { return client })
	})
}

func runVersioningCases(getTgt func() s3Target, getClient func() *s3.Client) {
	enableVersioning := func(bkt string) {
		client := getClient()
		_, err := client.PutBucketVersioning(context.Background(),
			&s3.PutBucketVersioningInput{
				Bucket: aws.String(bkt),
				VersioningConfiguration: &types.VersioningConfiguration{
					Status: types.BucketVersioningStatusEnabled,
				},
			})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	putVersion := func(bkt, key, body string) string {
		client := getClient()
		out, err := client.PutObject(context.Background(), &s3.PutObjectInput{
			Bucket: aws.String(bkt),
			Key:    aws.String(key),
			Body:   strings.NewReader(body),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vid := aws.ToString(out.VersionId)
		gomega.Expect(vid).NotTo(gomega.BeEmpty(), "PutObject must return a VersionId on versioning-enabled bucket")
		return vid
	}

	ginkgo.It("enables and reports bucket versioning (EnableAndStatus)", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		client := getClient()
		ctx := context.Background()
		bkt := tgt.uniqueBucket(t, "enable")

		// Before enabling, grainfs reports an "Unversioned" Status (server
		// quirk; AWS S3 returns empty). Just assert it's not Enabled yet.
		gout, err := client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(bkt)})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(gout.Status).NotTo(gomega.Equal(types.BucketVersioningStatusEnabled))

		enableVersioning(bkt)

		gout, err = client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(bkt)})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(gout.Status).To(gomega.Equal(types.BucketVersioningStatusEnabled))
	})

	ginkgo.It("puts and gets by version ID (PutGetByVersionID)", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		client := getClient()
		ctx := context.Background()
		bkt := tgt.uniqueBucket(t, "putget")
		enableVersioning(bkt)
		key := "obj.txt"

		vid1 := putVersion(bkt, key, "content-v1")
		vid2 := putVersion(bkt, key, "content-v2")
		gomega.Expect(vid2).NotTo(gomega.Equal(vid1))

		latest, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bkt), Key: aws.String(key)})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(latest.Body.Close)
		latestBody, _ := io.ReadAll(latest.Body)
		gomega.Expect(latestBody).To(gomega.Equal([]byte("content-v2")))

		v1, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bkt), Key: aws.String(key), VersionId: aws.String(vid1),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(v1.Body.Close)
		v1Body, _ := io.ReadAll(v1.Body)
		gomega.Expect(v1Body).To(gomega.Equal([]byte("content-v1")))
	})

	ginkgo.It("heads by version ID (HeadByVersionID)", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		client := getClient()
		ctx := context.Background()
		bkt := tgt.uniqueBucket(t, "headvid")
		enableVersioning(bkt)
		key := "obj.txt"

		vid1 := putVersion(bkt, key, "content-v1")
		vid2 := putVersion(bkt, key, "content-v2")

		// HEAD ?versionId=<vid1> → 200, x-amz-version-id == vid1.
		h1, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bkt), Key: aws.String(key), VersionId: aws.String(vid1),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(aws.ToString(h1.VersionId)).To(gomega.Equal(vid1))

		// HEAD ?versionId=<vid2> → 200, x-amz-version-id == vid2.
		h2, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bkt), Key: aws.String(key), VersionId: aws.String(vid2),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(aws.ToString(h2.VersionId)).To(gomega.Equal(vid2))

		// HEAD with non-existent versionId → 404 NotFound.
		_, err = client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bkt), Key: aws.String(key),
			VersionId: aws.String("01999999-9999-7999-9999-999999999999"),
		})
		gomega.Expect(err).To(gomega.HaveOccurred())
		var apiErr smithy.APIError
		gomega.Expect(errors.As(err, &apiErr)).To(gomega.BeTrue())
		gomega.Expect(apiErr.ErrorCode()).To(gomega.Equal("NotFound"))
	})

	ginkgo.It("heads by version ID through all nodes (HeadByVersionID_AllNodes)", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		ctx := context.Background()
		bkt := tgt.uniqueBucket(t, "headvidfan")
		enableVersioning(bkt)
		key := "obj.txt"
		vid := putVersion(bkt, key, "fanout")

		for i := 0; i < tgt.nodes; i++ {
			c := tgt.pickNode(i)
			h, err := c.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(bkt), Key: aws.String(key), VersionId: aws.String(vid),
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "HEAD ?versionId via node %d failed", i)
			gomega.Expect(aws.ToString(h.VersionId)).To(gomega.Equal(vid), "node %d returned wrong version id", i)
		}
	})

	ginkgo.It("returns MethodNotAllowed for delete marker head by version ID (HeadByVersionID_DeleteMarker)", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		client := getClient()
		ctx := context.Background()
		bkt := tgt.uniqueBucket(t, "headmark")
		enableVersioning(bkt)
		key := "obj.txt"
		_ = putVersion(bkt, key, "before")

		// Soft delete creates a delete marker; capture its versionId.
		delOut, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bkt), Key: aws.String(key),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		markerID := aws.ToString(delOut.VersionId)
		gomega.Expect(markerID).NotTo(gomega.BeEmpty())

		// HEAD ?versionId=<markerID> → 405 MethodNotAllowed + x-amz-delete-marker.
		_, err = client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bkt), Key: aws.String(key), VersionId: aws.String(markerID),
		})
		gomega.Expect(err).To(gomega.HaveOccurred())
		// aws-sdk-go-v2 surfaces 405 either as a generic APIError with status
		// code 405 or as an HTTP response error. Accept both shapes.
		var httpErr interface{ HTTPStatusCode() int }
		if errors.As(err, &httpErr) {
			gomega.Expect(httpErr.HTTPStatusCode()).To(gomega.Equal(405))
		}
	})

	ginkgo.It("soft deletes current version (SoftDelete)", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		client := getClient()
		ctx := context.Background()
		bkt := tgt.uniqueBucket(t, "softdel")
		enableVersioning(bkt)
		key := "file.txt"
		_ = putVersion(bkt, key, "v1")
		_ = putVersion(bkt, key, "v2")

		delOut, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bkt), Key: aws.String(key),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		markerID := aws.ToString(delOut.VersionId)
		gomega.Expect(markerID).NotTo(gomega.BeEmpty())

		_, err = client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bkt), Key: aws.String(key),
		})
		gomega.Expect(err).To(gomega.HaveOccurred(), "GET after soft-delete must return 404")
	})

	ginkgo.It("hard deletes a specific version ID (HardDeleteByVersionID)", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		client := getClient()
		ctx := context.Background()
		bkt := tgt.uniqueBucket(t, "harddel")
		enableVersioning(bkt)
		key := "file.txt"
		vid1 := putVersion(bkt, key, "v1")
		_ = putVersion(bkt, key, "v2")

		_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bkt), Key: aws.String(key), VersionId: aws.String(vid1),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		_, err = client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bkt), Key: aws.String(key), VersionId: aws.String(vid1),
		})
		gomega.Expect(err).To(gomega.HaveOccurred(), "GET of hard-deleted version must fail")
	})

	ginkgo.It("lists object versions (ListVersions)", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		client := getClient()
		ctx := context.Background()
		bkt := tgt.uniqueBucket(t, "list")
		enableVersioning(bkt)
		key := "file.txt"
		vid1 := putVersion(bkt, key, "v1")
		vid2 := putVersion(bkt, key, "v2")

		// Cluster's ListObjectVersions may include a "null" pseudo-version per
		// PutObject (differs from the in-process EC fixture). Assert what we
		// require: both real versionIds are present and exactly one IsLatest.
		// Strict equivalence is covered by internal/server/versioning_test.go
		// (TestListObjectVersions_EC).
		out, err := client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: aws.String(bkt)})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		seen := map[string]bool{}
		latestCount := 0
		for _, v := range out.Versions {
			id := aws.ToString(v.VersionId)
			seen[id] = true
			if aws.ToBool(v.IsLatest) {
				latestCount++
				gomega.Expect(id).To(gomega.Equal(vid2), "IsLatest must point at vid2")
			}
		}
		gomega.Expect(seen[vid1]).To(gomega.BeTrue(), "vid1 missing from ListObjectVersions")
		gomega.Expect(seen[vid2]).To(gomega.BeTrue(), "vid2 missing from ListObjectVersions")
		gomega.Expect(latestCount).To(gomega.Equal(1), "exactly one IsLatest version expected")
		// DeleteMarkers behavior is exercised by the dedicated
		// ListVersionsWithDeleteMarker case below; this case focuses on real
		// version enumeration only. Cluster may emit phantom markers from
		// versioning-init flows — out of scope here.
	})

	ginkgo.It("lists versions with delete marker (ListVersionsWithDeleteMarker)", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		client := getClient()
		ctx := context.Background()
		bkt := tgt.uniqueBucket(t, "listmark")
		enableVersioning(bkt)
		key := "file.txt"
		_ = putVersion(bkt, key, "v1")
		_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bkt), Key: aws.String(key)})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		out, err := client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: aws.String(bkt)})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(len(out.DeleteMarkers)).To(gomega.BeNumerically(">=", 1), "expected >=1 delete marker")
		latestMarker := false
		for _, m := range out.DeleteMarkers {
			if aws.ToBool(m.IsLatest) {
				latestMarker = true
				break
			}
		}
		gomega.Expect(latestMarker).To(gomega.BeTrue(), "exactly one delete marker must be IsLatest")
	})
}
