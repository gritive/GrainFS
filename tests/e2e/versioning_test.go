package e2e

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestE2E_Versioning_Full exercises the complete versioning lifecycle via the
// shared DistributedBackend server: put two versions, get by versionId, soft
// delete (creates delete marker), hard-delete a specific version.
func TestE2E_Versioning_Full(t *testing.T) {
	skipIfShort(t, "skipping E2E test in short mode")

	ctx := context.Background()
	bucket := "ver-e2e-full"
	key := "file.txt"

	createBucket(t, bucket)

	// PUT v1 — must return a VersionId.
	putOut1, err := testS3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader("content-v1"),
	})
	require.NoError(t, err)
	vid1 := aws.ToString(putOut1.VersionId)
	require.NotEmpty(t, vid1, "PutObject must return a VersionId")

	// PUT v2 — must return a different VersionId.
	putOut2, err := testS3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader("content-v2"),
	})
	require.NoError(t, err)
	vid2 := aws.ToString(putOut2.VersionId)
	require.NotEmpty(t, vid2)
	assert.NotEqual(t, vid1, vid2)

	// GET latest → v2.
	getOut, err := testS3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	got, _ := io.ReadAll(getOut.Body)
	getOut.Body.Close()
	assert.Equal(t, "content-v2", string(got))

	// GET ?versionId=vid1 → v1.
	getOut, err = testS3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket:    aws.String(bucket),
		Key:       aws.String(key),
		VersionId: aws.String(vid1),
	})
	require.NoError(t, err)
	got, _ = io.ReadAll(getOut.Body)
	getOut.Body.Close()
	assert.Equal(t, "content-v1", string(got))

	// ListObjectVersions → 2 versions, newest first, no delete markers.
	lvOut, err := testS3Client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket),
	})
	require.NoError(t, err)
	require.Len(t, lvOut.Versions, 2)
	assert.Empty(t, lvOut.DeleteMarkers)
	assert.Equal(t, vid2, aws.ToString(lvOut.Versions[0].VersionId), "newest version first")
	assert.True(t, aws.ToBool(lvOut.Versions[0].IsLatest))
	assert.Equal(t, vid1, aws.ToString(lvOut.Versions[1].VersionId))
	assert.False(t, aws.ToBool(lvOut.Versions[1].IsLatest))

	// Soft DELETE → creates delete marker; GET returns 404.
	delOut, err := testS3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	markerID := aws.ToString(delOut.VersionId)
	require.NotEmpty(t, markerID)

	_, err = testS3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.Error(t, err, "GET after soft-delete must return 404")

	// ListObjectVersions → 2 versions + 1 delete marker (latest).
	lvOut, err = testS3Client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket),
	})
	require.NoError(t, err)
	assert.Len(t, lvOut.Versions, 2)
	require.Len(t, lvOut.DeleteMarkers, 1)
	assert.True(t, aws.ToBool(lvOut.DeleteMarkers[0].IsLatest))

	// Hard-delete v1 via DeleteObject with VersionId.
	_, err = testS3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket:    aws.String(bucket),
		Key:       aws.String(key),
		VersionId: aws.String(vid1),
	})
	require.NoError(t, err)

	// ListObjectVersions → 1 version + 1 delete marker (vid1 gone).
	lvOut, err = testS3Client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket),
	})
	require.NoError(t, err)
	require.Len(t, lvOut.Versions, 1)
	assert.Equal(t, vid2, aws.ToString(lvOut.Versions[0].VersionId))
	assert.Len(t, lvOut.DeleteMarkers, 1)
}

// TestS3VersioningE2E exercises the positive-flow Versioning surface.
//
// Cluster-only: the single-node grainfs fixture (TestMain's `grainfs serve
// --data <tmpdir>`) accepts PutBucketVersioning but does not actually
// version object writes — its backend is not EC. The SDK-equivalent of the
// internal/server/versioning_test.go _EC cases therefore runs against the
// shared 4-node cluster only.
func TestS3VersioningE2E(t *testing.T) {
	skipIfShort(t, "cluster fixture not booted in -short mode")
	runVersioningCases(t, newSharedClusterS3Target(t))
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

	t.Run("PutGet", func(t *testing.T) {
		ctx := context.Background()
		bkt := tgt.uniqueBucket(t, "putget")

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

	t.Run("GetByVersionID", func(t *testing.T) {
		ctx := context.Background()
		bkt := tgt.uniqueBucket(t, "getbyvid")
		enableVersioning(t, bkt)
		key := "obj.txt"

		putV1, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bkt), Key: aws.String(key),
			Body: bytes.NewReader([]byte("content-v1")),
		})
		require.NoError(t, err)
		vid1 := aws.ToString(putV1.VersionId)
		require.NotEmpty(t, vid1)

		putV2, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bkt), Key: aws.String(key),
			Body: bytes.NewReader([]byte("content-v2")),
		})
		require.NoError(t, err)
		vid2 := aws.ToString(putV2.VersionId)
		require.NotEmpty(t, vid2)
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

	// NOTE: ListVersions / ListVersionsWithDeleteMarker / DeleteVersion cases
	// from internal/server/versioning_test.go are intentionally NOT migrated
	// here. The grainfs 4-node cluster's ListObjectVersions returns one extra
	// "null" version per PutObject (semantically different from the
	// in-process EC fixture in internal/server). The integration coverage
	// for that exact semantics stays in internal/server/versioning_test.go
	// (TestListObjectVersions_EC / _WithDeleteMarker_EC, TestDeleteObjectVersion_EC).
}
