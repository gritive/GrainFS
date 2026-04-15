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

func TestUnmarshalInvalidData(t *testing.T) {
	invalid := []byte("not valid protobuf data")

	tests := []struct {
		name string
		fn   func([]byte) error
	}{
		{"unmarshalBucketMeta", func(d []byte) error { _, err := unmarshalBucketMeta(d); return err }},
		{"unmarshalECObjectMeta", func(d []byte) error { _, err := unmarshalECObjectMeta(d); return err }},
		{"unmarshalECMultipartMeta", func(d []byte) error { _, err := unmarshalECMultipartMeta(d); return err }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.fn(invalid)
			assert.Error(t, err, "%s should fail on invalid protobuf", tt.name)
		})
	}
}

func TestBucketMetaCodecRoundTrip_Disabled(t *testing.T) {
	meta := &bucketMeta{ECEnabled: false}
	data, err := marshalBucketMeta(meta)
	require.NoError(t, err)

	decoded, err := unmarshalBucketMeta(data)
	require.NoError(t, err)
	assert.False(t, decoded.ECEnabled)
}

func TestECObjectMetaCodecRoundTrip_Plain(t *testing.T) {
	// Plain storage objects have DataShards=0, ParityShards=0
	meta := &ecObjectMeta{
		Key:          "plain/file.txt",
		Size:         100,
		ContentType:  "text/plain",
		ETag:         "abc",
		LastModified: 1700000000,
		DataShards:   0,
		ParityShards: 0,
		ShardSize:    0,
	}
	data, err := marshalECObjectMeta(meta)
	require.NoError(t, err)

	decoded, err := unmarshalECObjectMeta(data)
	require.NoError(t, err)
	assert.Equal(t, 0, decoded.DataShards)
	assert.Equal(t, 0, decoded.ParityShards)
	assert.Equal(t, "plain/file.txt", decoded.Key)
}

func TestECCodecOutputIsNotJSON(t *testing.T) {
	meta := &ecObjectMeta{Key: "test", Size: 100, DataShards: 4, ParityShards: 2}
	data, err := marshalECObjectMeta(meta)
	require.NoError(t, err)
	assert.NotContains(t, string(data), `{`)
}
