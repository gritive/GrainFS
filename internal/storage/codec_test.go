package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestObjectCodecRoundTrip(t *testing.T) {
	obj := &Object{
		Key:          "docs/readme.md",
		Size:         1024,
		ContentType:  "text/markdown",
		ETag:         "abc123",
		LastModified: 1700000000,
	}

	data, err := marshalObject(obj)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	decoded, err := unmarshalObject(data)
	require.NoError(t, err)
	assert.Equal(t, obj.Key, decoded.Key)
	assert.Equal(t, obj.Size, decoded.Size)
	assert.Equal(t, obj.ContentType, decoded.ContentType)
	assert.Equal(t, obj.ETag, decoded.ETag)
	assert.Equal(t, obj.LastModified, decoded.LastModified)
}

func TestMultipartMetaCodecRoundTrip(t *testing.T) {
	meta := &multipartMeta{
		UploadID:    "upload-123",
		Bucket:      "mybucket",
		Key:         "bigfile.bin",
		ContentType: "application/octet-stream",
		CreatedAt:   1700000000,
	}

	data, err := marshalMultipartMeta(meta)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	decoded, err := unmarshalMultipartMeta(data)
	require.NoError(t, err)
	assert.Equal(t, meta.UploadID, decoded.UploadID)
	assert.Equal(t, meta.Bucket, decoded.Bucket)
	assert.Equal(t, meta.Key, decoded.Key)
	assert.Equal(t, meta.ContentType, decoded.ContentType)
	assert.Equal(t, meta.CreatedAt, decoded.CreatedAt)
}

func TestCodecOutputIsNotJSON(t *testing.T) {
	obj := &Object{Key: "test.txt", Size: 100, ContentType: "text/plain", ETag: "xyz", LastModified: 1700000000}
	data, err := marshalObject(obj)
	require.NoError(t, err)

	str := string(data)
	assert.NotContains(t, str, `"key"`)
	assert.NotContains(t, str, `{`)
}
