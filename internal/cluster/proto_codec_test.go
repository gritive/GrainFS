package cluster

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeDecodeCommand_CreateBucket(t *testing.T) {
	orig := CreateBucketCmd{Bucket: "my-bucket"}

	encoded, err := EncodeCommand(CmdCreateBucket, orig)
	require.NoError(t, err)

	cmd, err := DecodeCommand(encoded)
	require.NoError(t, err)
	assert.Equal(t, CmdCreateBucket, cmd.Type)

	decoded, err := decodeCreateBucketCmd(cmd.Data)
	require.NoError(t, err)
	assert.Equal(t, "my-bucket", decoded.Bucket)
}

func TestEncodeDecodeCommand_PutObjectMeta(t *testing.T) {
	orig := PutObjectMetaCmd{
		Bucket:      "test-bucket",
		Key:         "photos/sunset.jpg",
		Size:        1048576,
		ContentType: "image/jpeg",
		ETag:        "d41d8cd98f00b204e9800998ecf8427e",
		ModTime:     1700000000,
	}

	encoded, err := EncodeCommand(CmdPutObjectMeta, orig)
	require.NoError(t, err)

	cmd, err := DecodeCommand(encoded)
	require.NoError(t, err)
	assert.Equal(t, CmdPutObjectMeta, cmd.Type)

	decoded, err := decodePutObjectMetaCmd(cmd.Data)
	require.NoError(t, err)
	assert.Equal(t, "test-bucket", decoded.Bucket)
	assert.Equal(t, "photos/sunset.jpg", decoded.Key)
	assert.Equal(t, int64(1048576), decoded.Size)
	assert.Equal(t, "image/jpeg", decoded.ContentType)
	assert.Equal(t, "d41d8cd98f00b204e9800998ecf8427e", decoded.ETag)
	assert.Equal(t, int64(1700000000), decoded.ModTime)
}

func TestEncodeDecodeCommand_CompleteMultipart(t *testing.T) {
	orig := CompleteMultipartCmd{
		Bucket:      "uploads",
		Key:         "video.mp4",
		UploadID:    "upload-abc-123",
		Size:        5242880,
		ContentType: "video/mp4",
		ETag:        "abc123def456",
		ModTime:     1700001000,
	}

	encoded, err := EncodeCommand(CmdCompleteMultipart, orig)
	require.NoError(t, err)

	cmd, err := DecodeCommand(encoded)
	require.NoError(t, err)
	assert.Equal(t, CmdCompleteMultipart, cmd.Type)

	decoded, err := decodeCompleteMultipartCmd(cmd.Data)
	require.NoError(t, err)
	assert.Equal(t, "uploads", decoded.Bucket)
	assert.Equal(t, "video.mp4", decoded.Key)
	assert.Equal(t, "upload-abc-123", decoded.UploadID)
	assert.Equal(t, int64(5242880), decoded.Size)
	assert.Equal(t, "video/mp4", decoded.ContentType)
	assert.Equal(t, "abc123def456", decoded.ETag)
	assert.Equal(t, int64(1700001000), decoded.ModTime)
}

func TestObjectMetaCodecRoundTrip(t *testing.T) {
	orig := objectMeta{
		Key:          "docs/readme.md",
		Size:         4096,
		ContentType:  "text/markdown",
		ETag:         "etag-xyz",
		LastModified: 1700002000,
	}

	data, err := marshalObjectMeta(orig)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	decoded, err := unmarshalObjectMeta(data)
	require.NoError(t, err)
	assert.Equal(t, orig.Key, decoded.Key)
	assert.Equal(t, orig.Size, decoded.Size)
	assert.Equal(t, orig.ContentType, decoded.ContentType)
	assert.Equal(t, orig.ETag, decoded.ETag)
	assert.Equal(t, orig.LastModified, decoded.LastModified)
}

func TestSnapshotStateCodecRoundTrip(t *testing.T) {
	orig := map[string][]byte{
		"bucket:docs":       []byte("{}"),
		"obj:docs/file.txt": []byte("meta-bytes"),
		"mpu:upload-1":      []byte("mpu-bytes"),
	}

	data, err := marshalSnapshotState(orig)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	decoded, err := unmarshalSnapshotState(data)
	require.NoError(t, err)
	assert.Len(t, decoded, 3)
	assert.Equal(t, []byte("{}"), decoded["bucket:docs"])
	assert.Equal(t, []byte("meta-bytes"), decoded["obj:docs/file.txt"])
	assert.Equal(t, []byte("mpu-bytes"), decoded["mpu:upload-1"])
}

func TestClusterMultipartMetaCodecRoundTrip(t *testing.T) {
	orig := clusterMultipartMeta{
		ContentType: "application/octet-stream",
	}

	data, err := marshalClusterMultipartMeta(orig)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	decoded, err := unmarshalClusterMultipartMeta(data)
	require.NoError(t, err)
	assert.Equal(t, "application/octet-stream", decoded.ContentType)
}

func TestClusterCodecOutputIsNotJSON(t *testing.T) {
	// Encode a command and verify the output is not valid JSON
	encoded, err := EncodeCommand(CmdCreateBucket, CreateBucketCmd{Bucket: "test"})
	require.NoError(t, err)

	var js json.RawMessage
	err = json.Unmarshal(encoded, &js)
	assert.Error(t, err, "protobuf output should not parse as valid JSON")

	// Also verify objectMeta encoding is not JSON
	metaBytes, err := marshalObjectMeta(objectMeta{
		Key: "key", Size: 10, ContentType: "text/plain", ETag: "e", LastModified: 1,
	})
	require.NoError(t, err)
	err = json.Unmarshal(metaBytes, &js)
	assert.Error(t, err, "protobuf objectMeta output should not parse as valid JSON")

	// Snapshot state encoding is not JSON
	snapBytes, err := marshalSnapshotState(map[string][]byte{"k": []byte("v")})
	require.NoError(t, err)
	err = json.Unmarshal(snapBytes, &js)
	assert.Error(t, err, "protobuf snapshot output should not parse as valid JSON")
}
