package storage_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

func TestCreateAndCompleteMultipartUpload(t *testing.T) {
	b := newBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "test-bucket"))

	upload, err := b.CreateMultipartUpload(context.Background(), "test-bucket", "big-file.bin", "application/octet-stream")
	require.NoError(t, err, "CreateMultipartUpload")
	assert.NotEmpty(t, upload.UploadID)

	part1Data := bytes.Repeat([]byte("A"), 5*1024*1024) // 5MB
	part2Data := bytes.Repeat([]byte("B"), 3*1024*1024) // 3MB

	p1, err := b.UploadPart(context.Background(), "test-bucket", "big-file.bin", upload.UploadID, 1, bytes.NewReader(part1Data), "")
	require.NoError(t, err, "UploadPart 1")
	assert.Equal(t, 1, p1.PartNumber)
	assert.NotEmpty(t, p1.ETag)

	p2, err := b.UploadPart(context.Background(), "test-bucket", "big-file.bin", upload.UploadID, 2, bytes.NewReader(part2Data), "")
	require.NoError(t, err, "UploadPart 2")

	obj, err := b.CompleteMultipartUpload(context.Background(), "test-bucket", "big-file.bin", upload.UploadID, []storage.Part{*p1, *p2})
	require.NoError(t, err, "CompleteMultipartUpload")
	assert.Equal(t, int64(len(part1Data)+len(part2Data)), obj.Size)

	// verify the completed object is readable
	rc, meta, err := b.GetObject(context.Background(), "test-bucket", "big-file.bin")
	require.NoError(t, err, "GetObject")
	defer rc.Close()
	data, _ := io.ReadAll(rc)
	assert.Len(t, data, len(part1Data)+len(part2Data))
	assert.Equal(t, "application/octet-stream", meta.ContentType)
}

// TestCompleteMultipartUploadPersistsParts verifies the backend persists
// Object.Parts so that a subsequent HeadObject can resolve ?partNumber=N.
func TestCompleteMultipartUploadPersistsParts(t *testing.T) {
	b := newBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "test-bucket"))

	upload, err := b.CreateMultipartUpload(context.Background(), "test-bucket", "parts.bin", "application/octet-stream")
	require.NoError(t, err)

	part1 := bytes.Repeat([]byte("A"), 5*1024*1024)
	part2 := bytes.Repeat([]byte("B"), 3*1024*1024)
	p1, err := b.UploadPart(context.Background(), "test-bucket", "parts.bin", upload.UploadID, 1, bytes.NewReader(part1), "")
	require.NoError(t, err)
	p2, err := b.UploadPart(context.Background(), "test-bucket", "parts.bin", upload.UploadID, 2, bytes.NewReader(part2), "")
	require.NoError(t, err)

	_, err = b.CompleteMultipartUpload(context.Background(), "test-bucket", "parts.bin", upload.UploadID, []storage.Part{*p1, *p2})
	require.NoError(t, err)

	// Head AFTER Complete — must read back Parts through the codec round-trip.
	head, err := b.HeadObject(context.Background(), "test-bucket", "parts.bin")
	require.NoError(t, err)
	require.Len(t, head.Parts, 2, "Parts must persist for partNumber resolution")
	assert.Equal(t, 1, head.Parts[0].PartNumber)
	assert.Equal(t, int64(len(part1)), head.Parts[0].Size)
	assert.Equal(t, p1.ETag, head.Parts[0].ETag)
	assert.Equal(t, 2, head.Parts[1].PartNumber)
	assert.Equal(t, int64(len(part2)), head.Parts[1].Size)
	assert.Equal(t, p2.ETag, head.Parts[1].ETag)
}

func TestAbortMultipartUpload(t *testing.T) {
	b := newBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "test-bucket"))

	upload, _ := b.CreateMultipartUpload(context.Background(), "test-bucket", "aborted.bin", "application/octet-stream")
	_, _ = b.UploadPart(context.Background(), "test-bucket", "aborted.bin", upload.UploadID, 1, bytes.NewReader([]byte("data")), "")

	require.NoError(t, b.AbortMultipartUpload(context.Background(), "test-bucket", "aborted.bin", upload.UploadID), "AbortMultipartUpload")

	// object should not exist
	_, err := b.HeadObject(context.Background(), "test-bucket", "aborted.bin")
	require.ErrorIs(t, err, storage.ErrObjectNotFound)

	// abort again should fail
	require.ErrorIs(t, b.AbortMultipartUpload(context.Background(), "test-bucket", "aborted.bin", upload.UploadID), storage.ErrUploadNotFound)
}

func TestUploadPartInvalidUploadID(t *testing.T) {
	b := newBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "test-bucket"))

	_, err := b.UploadPart(context.Background(), "test-bucket", "file.bin", "invalid-id", 1, bytes.NewReader([]byte("data")), "")
	require.ErrorIs(t, err, storage.ErrUploadNotFound)
}

func TestCompleteMultipartBucketNotFound(t *testing.T) {
	b := newBackend(t)

	_, err := b.CreateMultipartUpload(context.Background(), "nope", "file.bin", "application/octet-stream")
	require.ErrorIs(t, err, storage.ErrBucketNotFound)
}

func TestMultipartListUploads(t *testing.T) {
	b := newBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "alpha"))
	require.NoError(t, b.CreateBucket(ctx, "beta"))

	mk := func(bucket, key string) string {
		up, err := b.CreateMultipartUpload(ctx, bucket, key, "application/octet-stream")
		require.NoError(t, err)
		return up.UploadID
	}
	idA1 := mk("alpha", "logs/2026-05-08")
	idA2 := mk("alpha", "logs/2026-05-09")
	idA3 := mk("alpha", "snapshots/2026-05-08")
	idB := mk("beta", "logs/2026-05-08")

	uploadsA, err := b.ListMultipartUploads(ctx, "alpha", "", 0)
	require.NoError(t, err)
	gotIDsA := make(map[string]bool)
	for _, u := range uploadsA {
		gotIDsA[u.UploadID] = true
		assert.Equal(t, "alpha", u.Bucket)
	}
	assert.True(t, gotIDsA[idA1])
	assert.True(t, gotIDsA[idA2])
	assert.True(t, gotIDsA[idA3])
	assert.False(t, gotIDsA[idB], "beta upload must not appear in alpha listing")

	uploadsB, err := b.ListMultipartUploads(ctx, "beta", "", 0)
	require.NoError(t, err)
	require.Len(t, uploadsB, 1)
	assert.Equal(t, idB, uploadsB[0].UploadID)

	uploadsLogs, err := b.ListMultipartUploads(ctx, "alpha", "logs/", 0)
	require.NoError(t, err)
	gotPrefix := make(map[string]bool)
	for _, u := range uploadsLogs {
		gotPrefix[u.UploadID] = true
	}
	assert.True(t, gotPrefix[idA1])
	assert.True(t, gotPrefix[idA2])
	assert.False(t, gotPrefix[idA3], "snapshots/ upload must not match logs/ prefix")

	uploadsCap, err := b.ListMultipartUploads(ctx, "alpha", "", 2)
	require.NoError(t, err)
	assert.Len(t, uploadsCap, 2, "maxUploads cap must apply")
}

func TestMultipartListParts(t *testing.T) {
	b := newBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "mybucket"))
	upload, err := b.CreateMultipartUpload(ctx, "mybucket", "obj.bin", "application/octet-stream")
	require.NoError(t, err)
	uploadID := upload.UploadID

	empty, err := b.ListParts(ctx, "mybucket", "obj.bin", uploadID, 0)
	require.NoError(t, err)
	assert.Empty(t, empty, "no parts yet")

	wrote := map[int]string{}
	for _, n := range []int{2, 1, 3} {
		data := bytes.Repeat([]byte{byte('a' + n)}, 1024)
		p, err := b.UploadPart(ctx, "mybucket", "obj.bin", uploadID, n, bytes.NewReader(data), "")
		require.NoError(t, err)
		wrote[n] = p.ETag
	}

	parts, err := b.ListParts(ctx, "mybucket", "obj.bin", uploadID, 0)
	require.NoError(t, err)
	require.Len(t, parts, 3)
	for i, p := range parts {
		assert.Equal(t, i+1, p.PartNumber, "parts must be sorted ascending")
		assert.Equal(t, wrote[p.PartNumber], p.ETag, "ETag must match upload-time hash")
		assert.Equal(t, int64(1024), p.Size)
	}

	capped, err := b.ListParts(ctx, "mybucket", "obj.bin", uploadID, 2)
	require.NoError(t, err)
	assert.Len(t, capped, 2)
	assert.Equal(t, 1, capped[0].PartNumber)
	assert.Equal(t, 2, capped[1].PartNumber)
}

func TestMultipartListPartsNotFound(t *testing.T) {
	b := newBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "mybucket"))
	_, err := b.ListParts(ctx, "mybucket", "ghost.bin", "ghost-upload-id", 0)
	require.ErrorIs(t, err, storage.ErrUploadNotFound)
}
