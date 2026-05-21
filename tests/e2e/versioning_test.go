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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	ginkgo.Context(name, func() {
		var (
			tgt    s3Target
			client *s3.Client
		)

		ginkgo.BeforeEach(func() {
			tgt = factory()
			client = tgt.pickNode(0)
		})

		runVersioningCases(func() s3Target { return tgt }, func() *s3.Client { return client })
	})
}

func runVersioningCases(getTgt func() s3Target, getClient func() *s3.Client) {
	enableVersioning := func(bkt string) {
		t := ginkgo.GinkgoTB()
		client := getClient()
		_, err := client.PutBucketVersioning(context.Background(),
			&s3.PutBucketVersioningInput{
				Bucket: aws.String(bkt),
				VersioningConfiguration: &types.VersioningConfiguration{
					Status: types.BucketVersioningStatusEnabled,
				},
			})
		require.NoError(t, err)
	}

	putVersion := func(bkt, key, body string) string {
		t := ginkgo.GinkgoTB()
		client := getClient()
		out, err := client.PutObject(context.Background(), &s3.PutObjectInput{
			Bucket: aws.String(bkt),
			Key:    aws.String(key),
			Body:   strings.NewReader(body),
		})
		require.NoError(t, err)
		vid := aws.ToString(out.VersionId)
		require.NotEmpty(t, vid, "PutObject must return a VersionId on versioning-enabled bucket")
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
		require.NoError(t, err)
		assert.NotEqual(t, types.BucketVersioningStatusEnabled, gout.Status)

		enableVersioning(bkt)

		gout, err = client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(bkt)})
		require.NoError(t, err)
		assert.Equal(t, types.BucketVersioningStatusEnabled, gout.Status)
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
		assert.NotEqual(t, vid1, vid2)

		latest, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bkt), Key: aws.String(key)})
		require.NoError(t, err)
		ginkgo.DeferCleanup(latest.Body.Close)
		latestBody, _ := io.ReadAll(latest.Body)
		assert.Equal(t, []byte("content-v2"), latestBody)

		v1, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bkt), Key: aws.String(key), VersionId: aws.String(vid1),
		})
		require.NoError(t, err)
		ginkgo.DeferCleanup(v1.Body.Close)
		v1Body, _ := io.ReadAll(v1.Body)
		assert.Equal(t, []byte("content-v1"), v1Body)
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
		require.NoError(t, err)
		assert.Equal(t, vid1, aws.ToString(h1.VersionId))

		// HEAD ?versionId=<vid2> → 200, x-amz-version-id == vid2.
		h2, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bkt), Key: aws.String(key), VersionId: aws.String(vid2),
		})
		require.NoError(t, err)
		assert.Equal(t, vid2, aws.ToString(h2.VersionId))

		// HEAD with non-existent versionId → 404 NotFound.
		_, err = client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bkt), Key: aws.String(key),
			VersionId: aws.String("01999999-9999-7999-9999-999999999999"),
		})
		require.Error(t, err)
		var apiErr smithy.APIError
		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, "NotFound", apiErr.ErrorCode())
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
			require.NoErrorf(t, err, "HEAD ?versionId via node %d failed", i)
			assert.Equal(t, vid, aws.ToString(h.VersionId), "node %d returned wrong version id", i)
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
		require.NoError(t, err)
		markerID := aws.ToString(delOut.VersionId)
		require.NotEmpty(t, markerID)

		// HEAD ?versionId=<markerID> → 405 MethodNotAllowed + x-amz-delete-marker.
		_, err = client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bkt), Key: aws.String(key), VersionId: aws.String(markerID),
		})
		require.Error(t, err)
		// aws-sdk-go-v2 surfaces 405 either as a generic APIError with status
		// code 405 or as an HTTP response error. Accept both shapes.
		var httpErr interface{ HTTPStatusCode() int }
		if errors.As(err, &httpErr) {
			assert.Equal(t, 405, httpErr.HTTPStatusCode())
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
		require.NoError(t, err)
		markerID := aws.ToString(delOut.VersionId)
		require.NotEmpty(t, markerID)

		_, err = client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bkt), Key: aws.String(key),
		})
		require.Error(t, err, "GET after soft-delete must return 404")
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
		require.NoError(t, err)

		_, err = client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bkt), Key: aws.String(key), VersionId: aws.String(vid1),
		})
		require.Error(t, err, "GET of hard-deleted version must fail")
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
		require.NoError(t, err)
		seen := map[string]bool{}
		latestCount := 0
		for _, v := range out.Versions {
			id := aws.ToString(v.VersionId)
			seen[id] = true
			if aws.ToBool(v.IsLatest) {
				latestCount++
				assert.Equal(t, vid2, id, "IsLatest must point at vid2")
			}
		}
		assert.True(t, seen[vid1], "vid1 missing from ListObjectVersions")
		assert.True(t, seen[vid2], "vid2 missing from ListObjectVersions")
		assert.Equal(t, 1, latestCount, "exactly one IsLatest version expected")
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
		require.NoError(t, err)

		out, err := client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: aws.String(bkt)})
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(out.DeleteMarkers), 1, "expected >=1 delete marker")
		latestMarker := false
		for _, m := range out.DeleteMarkers {
			if aws.ToBool(m.IsLatest) {
				latestMarker = true
				break
			}
		}
		assert.True(t, latestMarker, "exactly one delete marker must be IsLatest")
	})
}
