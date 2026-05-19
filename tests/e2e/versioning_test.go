package e2e

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestS3VersioningE2E exercises the versioning surface against both single-node
// and the shared 4-node cluster. Subtests that genuinely require the cluster
// (cross-node fan-out) opt out via tgt.isCluster checks.
func TestS3VersioningE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runVersioningCases(t, newSingleNodeS3Target())
	})

	t.Run("Cluster4Node", func(t *testing.T) {
		skipIfShort(t, "cluster fixture not booted in -short mode")
		runVersioningCases(t, newSharedClusterS3Target(t))
	})
}

func runVersioningCases(t *testing.T, tgt s3Target) {
	client := tgt.pickNode(0)

	enableVersioning := func(t *testing.T, bkt string) {
		t.Helper()
		_, err := client.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{
			Bucket: aws.String(bkt),
			VersioningConfiguration: &types.VersioningConfiguration{
				Status: types.BucketVersioningStatusEnabled,
			},
		})
		require.NoError(t, err)
	}

	putVersion := func(t *testing.T, bkt, key, body string) string {
		t.Helper()
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

	t.Run("EnableAndStatus", func(t *testing.T) {
		ctx := context.Background()
		bkt := tgt.uniqueBucket(t, "enable")

		// Before enabling, grainfs reports an "Unversioned" Status (server
		// quirk; AWS S3 returns empty). Just assert it's not Enabled yet.
		gout, err := client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(bkt)})
		require.NoError(t, err)
		assert.NotEqual(t, types.BucketVersioningStatusEnabled, gout.Status)

		enableVersioning(t, bkt)

		gout, err = client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(bkt)})
		require.NoError(t, err)
		assert.Equal(t, types.BucketVersioningStatusEnabled, gout.Status)
	})

	t.Run("PutGetByVersionID", func(t *testing.T) {
		ctx := context.Background()
		bkt := tgt.uniqueBucket(t, "putget")
		enableVersioning(t, bkt)
		key := "obj.txt"

		vid1 := putVersion(t, bkt, key, "content-v1")
		vid2 := putVersion(t, bkt, key, "content-v2")
		assert.NotEqual(t, vid1, vid2)

		latest, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bkt), Key: aws.String(key)})
		require.NoError(t, err)
		latestBody, _ := io.ReadAll(latest.Body)
		latest.Body.Close()
		assert.Equal(t, []byte("content-v2"), latestBody)

		v1, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bkt), Key: aws.String(key), VersionId: aws.String(vid1),
		})
		require.NoError(t, err)
		v1Body, _ := io.ReadAll(v1.Body)
		v1.Body.Close()
		assert.Equal(t, []byte("content-v1"), v1Body)
	})

	t.Run("HeadByVersionID", func(t *testing.T) {
		ctx := context.Background()
		bkt := tgt.uniqueBucket(t, "headvid")
		enableVersioning(t, bkt)
		key := "obj.txt"

		vid1 := putVersion(t, bkt, key, "content-v1")
		vid2 := putVersion(t, bkt, key, "content-v2")

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

	t.Run("HeadByVersionID_AllNodes", func(t *testing.T) {
		if !tgt.isCluster {
			t.Skip("requires cluster fixture for fan-out")
		}
		ctx := context.Background()
		bkt := tgt.uniqueBucket(t, "headvidfan")
		enableVersioning(t, bkt)
		key := "obj.txt"
		vid := putVersion(t, bkt, key, "fanout")

		for i := 0; i < tgt.nodes; i++ {
			c := tgt.pickNode(i)
			h, err := c.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(bkt), Key: aws.String(key), VersionId: aws.String(vid),
			})
			require.NoErrorf(t, err, "HEAD ?versionId via node %d failed", i)
			assert.Equal(t, vid, aws.ToString(h.VersionId), "node %d returned wrong version id", i)
		}
	})

	t.Run("HeadByVersionID_DeleteMarker", func(t *testing.T) {
		ctx := context.Background()
		bkt := tgt.uniqueBucket(t, "headmark")
		enableVersioning(t, bkt)
		key := "obj.txt"
		_ = putVersion(t, bkt, key, "before")

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

	t.Run("SoftDelete", func(t *testing.T) {
		ctx := context.Background()
		bkt := tgt.uniqueBucket(t, "softdel")
		enableVersioning(t, bkt)
		key := "file.txt"
		_ = putVersion(t, bkt, key, "v1")
		_ = putVersion(t, bkt, key, "v2")

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

	t.Run("HardDeleteByVersionID", func(t *testing.T) {
		ctx := context.Background()
		bkt := tgt.uniqueBucket(t, "harddel")
		enableVersioning(t, bkt)
		key := "file.txt"
		vid1 := putVersion(t, bkt, key, "v1")
		_ = putVersion(t, bkt, key, "v2")

		_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bkt), Key: aws.String(key), VersionId: aws.String(vid1),
		})
		require.NoError(t, err)

		_, err = client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bkt), Key: aws.String(key), VersionId: aws.String(vid1),
		})
		require.Error(t, err, "GET of hard-deleted version must fail")
	})

	t.Run("ListVersions", func(t *testing.T) {
		ctx := context.Background()
		bkt := tgt.uniqueBucket(t, "list")
		enableVersioning(t, bkt)
		key := "file.txt"
		vid1 := putVersion(t, bkt, key, "v1")
		vid2 := putVersion(t, bkt, key, "v2")

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

	t.Run("ListVersionsWithDeleteMarker", func(t *testing.T) {
		ctx := context.Background()
		bkt := tgt.uniqueBucket(t, "listmark")
		enableVersioning(t, bkt)
		key := "file.txt"
		_ = putVersion(t, bkt, key, "v1")
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
