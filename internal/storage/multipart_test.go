package storage

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateAndCompleteMultipartUpload(t *testing.T) {
	b := setupTestBackend(t)
	b.CreateBucket("test-bucket")

	upload, err := b.CreateMultipartUpload("test-bucket", "big-file.bin", "application/octet-stream")
	require.NoError(t, err, "CreateMultipartUpload")
	assert.NotEmpty(t, upload.UploadID)

	part1Data := bytes.Repeat([]byte("A"), 5*1024*1024) // 5MB
	part2Data := bytes.Repeat([]byte("B"), 3*1024*1024) // 3MB

	p1, err := b.UploadPart("test-bucket", "big-file.bin", upload.UploadID, 1, bytes.NewReader(part1Data))
	require.NoError(t, err, "UploadPart 1")
	assert.Equal(t, 1, p1.PartNumber)
	assert.NotEmpty(t, p1.ETag)

	p2, err := b.UploadPart("test-bucket", "big-file.bin", upload.UploadID, 2, bytes.NewReader(part2Data))
	require.NoError(t, err, "UploadPart 2")

	obj, err := b.CompleteMultipartUpload("test-bucket", "big-file.bin", upload.UploadID, []Part{*p1, *p2})
	require.NoError(t, err, "CompleteMultipartUpload")
	assert.Equal(t, int64(len(part1Data)+len(part2Data)), obj.Size)

	// verify the completed object is readable
	rc, meta, err := b.GetObject("test-bucket", "big-file.bin")
	require.NoError(t, err, "GetObject")
	defer rc.Close()
	data, _ := io.ReadAll(rc)
	assert.Len(t, data, len(part1Data)+len(part2Data))
	assert.Equal(t, "application/octet-stream", meta.ContentType)
}

func TestAbortMultipartUpload(t *testing.T) {
	b := setupTestBackend(t)
	b.CreateBucket("test-bucket")

	upload, _ := b.CreateMultipartUpload("test-bucket", "aborted.bin", "application/octet-stream")
	b.UploadPart("test-bucket", "aborted.bin", upload.UploadID, 1, bytes.NewReader([]byte("data")))

	require.NoError(t, b.AbortMultipartUpload("test-bucket", "aborted.bin", upload.UploadID), "AbortMultipartUpload")

	// object should not exist
	_, err := b.HeadObject("test-bucket", "aborted.bin")
	require.ErrorIs(t, err, ErrObjectNotFound)

	// abort again should fail
	require.ErrorIs(t, b.AbortMultipartUpload("test-bucket", "aborted.bin", upload.UploadID), ErrUploadNotFound)
}

func TestUploadPartInvalidUploadID(t *testing.T) {
	b := setupTestBackend(t)
	b.CreateBucket("test-bucket")

	_, err := b.UploadPart("test-bucket", "file.bin", "invalid-id", 1, bytes.NewReader([]byte("data")))
	require.ErrorIs(t, err, ErrUploadNotFound)
}

func TestCompleteMultipartBucketNotFound(t *testing.T) {
	b := setupTestBackend(t)

	_, err := b.CreateMultipartUpload("nope", "file.bin", "application/octet-stream")
	require.ErrorIs(t, err, ErrBucketNotFound)
}
