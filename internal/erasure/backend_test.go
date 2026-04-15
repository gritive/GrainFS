package erasure

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

func newTestBackend(t *testing.T) *ECBackend {
	t.Helper()
	dir := t.TempDir()
	b, err := NewECBackend(dir, DefaultDataShards, DefaultParityShards)
	require.NoError(t, err)
	t.Cleanup(func() { b.Close() })
	return b
}

func TestECBackend_BucketOps(t *testing.T) {
	b := newTestBackend(t)

	require.NoError(t, b.CreateBucket("test"))
	require.NoError(t, b.HeadBucket("test"))

	buckets, err := b.ListBuckets()
	require.NoError(t, err)
	assert.Equal(t, []string{"test"}, buckets)

	assert.ErrorIs(t, b.CreateBucket("test"), storage.ErrBucketAlreadyExists)
	assert.ErrorIs(t, b.HeadBucket("nope"), storage.ErrBucketNotFound)

	require.NoError(t, b.DeleteBucket("test"))
	assert.ErrorIs(t, b.HeadBucket("test"), storage.ErrBucketNotFound)
}

func TestECBackend_PutGetRoundtrip(t *testing.T) {
	b := newTestBackend(t)
	require.NoError(t, b.CreateBucket("bucket"))

	tests := []struct {
		name    string
		key     string
		content string
	}{
		{"small", "small.txt", "hello"},
		{"medium", "medium.txt", strings.Repeat("x", 10000)},
		{"nested_key", "path/to/file.txt", "nested content"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			obj, err := b.PutObject("bucket", tt.key, strings.NewReader(tt.content), "text/plain")
			require.NoError(t, err)
			assert.Equal(t, int64(len(tt.content)), obj.Size)
			assert.NotEmpty(t, obj.ETag)

			rc, gotObj, err := b.GetObject("bucket", tt.key)
			require.NoError(t, err)
			defer rc.Close()

			data, _ := io.ReadAll(rc)
			assert.Equal(t, tt.content, string(data))
			assert.Equal(t, obj.ETag, gotObj.ETag)
			assert.Equal(t, obj.Size, gotObj.Size)
		})
	}
}

func TestECBackend_ShardRecovery(t *testing.T) {
	b := newTestBackend(t)
	require.NoError(t, b.CreateBucket("bucket"))

	content := bytes.Repeat([]byte("recovery test data "), 500)
	_, err := b.PutObject("bucket", "recover.bin", bytes.NewReader(content), "application/octet-stream")
	require.NoError(t, err)

	shardDir := b.ShardDir("bucket", "recover.bin")

	// Delete shard 0 and shard 5 (one data, one parity)
	os.Remove(shardDir + "/00")
	os.Remove(shardDir + "/05")

	rc, _, err := b.GetObject("bucket", "recover.bin")
	require.NoError(t, err)
	defer rc.Close()

	data, _ := io.ReadAll(rc)
	assert.Equal(t, content, data)
}

func TestECBackend_TooManyShardsLost(t *testing.T) {
	b := newTestBackend(t)
	require.NoError(t, b.CreateBucket("bucket"))

	content := []byte("this will be unrecoverable")
	_, err := b.PutObject("bucket", "lost.bin", bytes.NewReader(content), "application/octet-stream")
	require.NoError(t, err)

	shardDir := b.ShardDir("bucket", "lost.bin")
	os.Remove(shardDir + "/00")
	os.Remove(shardDir + "/01")
	os.Remove(shardDir + "/02")

	_, _, err = b.GetObject("bucket", "lost.bin")
	assert.Error(t, err)
}

func TestECBackend_DeleteObject(t *testing.T) {
	b := newTestBackend(t)
	require.NoError(t, b.CreateBucket("bucket"))

	_, err := b.PutObject("bucket", "del.txt", strings.NewReader("data"), "text/plain")
	require.NoError(t, err)

	require.NoError(t, b.DeleteObject("bucket", "del.txt"))

	_, _, err = b.GetObject("bucket", "del.txt")
	assert.Error(t, err)

	_, err = os.Stat(b.ShardDir("bucket", "del.txt"))
	assert.True(t, os.IsNotExist(err))
}

func TestECBackend_ListObjects(t *testing.T) {
	b := newTestBackend(t)
	require.NoError(t, b.CreateBucket("bucket"))

	for _, key := range []string{"a.txt", "b.txt", "dir/c.txt"} {
		_, err := b.PutObject("bucket", key, strings.NewReader("x"), "text/plain")
		require.NoError(t, err)
	}

	objects, err := b.ListObjects("bucket", "", 100)
	require.NoError(t, err)
	assert.Len(t, objects, 3)

	objects, err = b.ListObjects("bucket", "dir/", 100)
	require.NoError(t, err)
	assert.Len(t, objects, 1)
	assert.Equal(t, "dir/c.txt", objects[0].Key)
}

func TestECBackend_Overwrite(t *testing.T) {
	b := newTestBackend(t)
	require.NoError(t, b.CreateBucket("bucket"))

	_, err := b.PutObject("bucket", "file.txt", strings.NewReader("v1"), "text/plain")
	require.NoError(t, err)

	_, err = b.PutObject("bucket", "file.txt", strings.NewReader("version2"), "text/plain")
	require.NoError(t, err)

	rc, obj, err := b.GetObject("bucket", "file.txt")
	require.NoError(t, err)
	defer rc.Close()

	data, _ := io.ReadAll(rc)
	assert.Equal(t, "version2", string(data))
	assert.Equal(t, int64(8), obj.Size)
}

func TestECBackend_Multipart(t *testing.T) {
	b := newTestBackend(t)
	require.NoError(t, b.CreateBucket("bucket"))

	part1Data := bytes.Repeat([]byte("A"), 1024)
	part2Data := bytes.Repeat([]byte("B"), 512)

	upload, err := b.CreateMultipartUpload("bucket", "mp.bin", "application/octet-stream")
	require.NoError(t, err)

	p1, err := b.UploadPart("bucket", "mp.bin", upload.UploadID, 1, bytes.NewReader(part1Data))
	require.NoError(t, err)

	p2, err := b.UploadPart("bucket", "mp.bin", upload.UploadID, 2, bytes.NewReader(part2Data))
	require.NoError(t, err)

	obj, err := b.CompleteMultipartUpload("bucket", "mp.bin", upload.UploadID, []storage.Part{
		{PartNumber: p1.PartNumber, ETag: p1.ETag, Size: p1.Size},
		{PartNumber: p2.PartNumber, ETag: p2.ETag, Size: p2.Size},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(len(part1Data)+len(part2Data)), obj.Size)

	// Verify the assembled and EC-encoded object
	rc, _, err := b.GetObject("bucket", "mp.bin")
	require.NoError(t, err)
	defer rc.Close()

	data, _ := io.ReadAll(rc)
	expected := append(part1Data, part2Data...)
	assert.Equal(t, expected, data)
}

func TestECBackend_AbortMultipart(t *testing.T) {
	b := newTestBackend(t)
	require.NoError(t, b.CreateBucket("bucket"))

	upload, err := b.CreateMultipartUpload("bucket", "abort.bin", "application/octet-stream")
	require.NoError(t, err)

	_, err = b.UploadPart("bucket", "abort.bin", upload.UploadID, 1, strings.NewReader("data"))
	require.NoError(t, err)

	require.NoError(t, b.AbortMultipartUpload("bucket", "abort.bin", upload.UploadID))

	_, _, err = b.GetObject("bucket", "abort.bin")
	assert.Error(t, err)
}

func TestECBackend_DeleteNonexistentObject(t *testing.T) {
	b := newTestBackend(t)
	require.NoError(t, b.CreateBucket("bucket"))

	// S3 semantics: delete nonexistent is not an error
	err := b.DeleteObject("bucket", "nope.txt")
	assert.NoError(t, err)
}

func TestECBackend_DeleteBucketNotEmpty(t *testing.T) {
	b := newTestBackend(t)
	require.NoError(t, b.CreateBucket("bucket"))

	_, err := b.PutObject("bucket", "file.txt", strings.NewReader("data"), "text/plain")
	require.NoError(t, err)

	err = b.DeleteBucket("bucket")
	assert.ErrorIs(t, err, storage.ErrBucketNotEmpty)
}
