package e2e

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestE2E_Versioning_Full exercises the complete versioning lifecycle via the
// shared DistributedBackend server: put two versions, get by versionId, soft
// delete (creates delete marker), hard-delete a specific version.
func TestE2E_Versioning_Full(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

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
