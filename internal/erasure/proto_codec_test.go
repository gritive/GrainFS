package erasure

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBucketMetaCodecRoundTrip(t *testing.T) {
	meta := &bucketMeta{ECEnabled: true}
	data, err := marshalBucketMeta(meta)
	require.NoError(t, err)

	decoded, err := unmarshalBucketMeta(data)
	require.NoError(t, err)
	assert.Equal(t, meta.ECEnabled, decoded.ECEnabled)
}

func TestECObjectMetaCodecRoundTrip(t *testing.T) {
	meta := &ecObjectMeta{
		Key:          "docs/file.txt",
		Size:         2048,
		ContentType:  "text/plain",
		ETag:         "abc123",
		LastModified: 1700000000,
		DataShards:   4,
		ParityShards: 2,
		ShardSize:    512,
	}
	data, err := marshalECObjectMeta(meta)
	require.NoError(t, err)

	decoded, err := unmarshalECObjectMeta(data)
	require.NoError(t, err)
	assert.Equal(t, meta.Key, decoded.Key)
	assert.Equal(t, meta.Size, decoded.Size)
	assert.Equal(t, meta.DataShards, decoded.DataShards)
	assert.Equal(t, meta.ParityShards, decoded.ParityShards)
	assert.Equal(t, meta.ShardSize, decoded.ShardSize)
}

func TestECMultipartMetaCodecRoundTrip(t *testing.T) {
	meta := &ecMultipartMeta{
		UploadID:    "upload-456",
		Bucket:      "mybucket",
		Key:         "bigfile.bin",
		ContentType: "application/octet-stream",
		CreatedAt:   1700000000,
	}
	data, err := marshalECMultipartMeta(meta)
	require.NoError(t, err)

	decoded, err := unmarshalECMultipartMeta(data)
	require.NoError(t, err)
	assert.Equal(t, meta.UploadID, decoded.UploadID)
	assert.Equal(t, meta.Bucket, decoded.Bucket)
	assert.Equal(t, meta.ContentType, decoded.ContentType)
}

func TestECCodecOutputIsNotJSON(t *testing.T) {
	meta := &ecObjectMeta{Key: "test", Size: 100, DataShards: 4, ParityShards: 2}
	data, err := marshalECObjectMeta(meta)
	require.NoError(t, err)
	assert.NotContains(t, string(data), `{`)
}
