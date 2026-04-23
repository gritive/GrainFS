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

func TestEncodeDecodeCommand_DeleteBucket(t *testing.T) {
	orig := DeleteBucketCmd{Bucket: "remove-me"}

	encoded, err := EncodeCommand(CmdDeleteBucket, orig)
	require.NoError(t, err)

	cmd, err := DecodeCommand(encoded)
	require.NoError(t, err)
	assert.Equal(t, CmdDeleteBucket, cmd.Type)

	decoded, err := decodeDeleteBucketCmd(cmd.Data)
	require.NoError(t, err)
	assert.Equal(t, "remove-me", decoded.Bucket)
}

func TestEncodeDecodeCommand_DeleteObject(t *testing.T) {
	orig := DeleteObjectCmd{Bucket: "b", Key: "file.txt"}

	encoded, err := EncodeCommand(CmdDeleteObject, orig)
	require.NoError(t, err)

	cmd, err := DecodeCommand(encoded)
	require.NoError(t, err)
	assert.Equal(t, CmdDeleteObject, cmd.Type)

	decoded, err := decodeDeleteObjectCmd(cmd.Data)
	require.NoError(t, err)
	assert.Equal(t, "b", decoded.Bucket)
	assert.Equal(t, "file.txt", decoded.Key)
}

func TestEncodeDecodeCommand_CreateMultipartUpload(t *testing.T) {
	orig := CreateMultipartUploadCmd{
		UploadID: "mpu-123", Bucket: "b", Key: "big.bin",
		ContentType: "application/octet-stream", CreatedAt: 1700000000,
	}

	encoded, err := EncodeCommand(CmdCreateMultipartUpload, orig)
	require.NoError(t, err)

	cmd, err := DecodeCommand(encoded)
	require.NoError(t, err)
	assert.Equal(t, CmdCreateMultipartUpload, cmd.Type)

	decoded, err := decodeCreateMultipartUploadCmd(cmd.Data)
	require.NoError(t, err)
	assert.Equal(t, "mpu-123", decoded.UploadID)
	assert.Equal(t, "b", decoded.Bucket)
	assert.Equal(t, "big.bin", decoded.Key)
	assert.Equal(t, "application/octet-stream", decoded.ContentType)
	assert.Equal(t, int64(1700000000), decoded.CreatedAt)
}

func TestEncodeDecodeCommand_AbortMultipart(t *testing.T) {
	orig := AbortMultipartCmd{Bucket: "b", Key: "abort.bin", UploadID: "mpu-abort"}

	encoded, err := EncodeCommand(CmdAbortMultipart, orig)
	require.NoError(t, err)

	cmd, err := DecodeCommand(encoded)
	require.NoError(t, err)
	assert.Equal(t, CmdAbortMultipart, cmd.Type)

	decoded, err := decodeAbortMultipartCmd(cmd.Data)
	require.NoError(t, err)
	assert.Equal(t, "b", decoded.Bucket)
	assert.Equal(t, "abort.bin", decoded.Key)
	assert.Equal(t, "mpu-abort", decoded.UploadID)
}

func TestEncodeDecodeCommand_SetBucketPolicy(t *testing.T) {
	policyJSON := []byte(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow"}]}`)
	orig := SetBucketPolicyCmd{Bucket: "my-bucket", PolicyJSON: policyJSON}

	encoded, err := EncodeCommand(CmdSetBucketPolicy, orig)
	require.NoError(t, err)

	cmd, err := DecodeCommand(encoded)
	require.NoError(t, err)
	assert.Equal(t, CmdSetBucketPolicy, cmd.Type)

	decoded, err := decodeSetBucketPolicyCmd(cmd.Data)
	require.NoError(t, err)
	assert.Equal(t, "my-bucket", decoded.Bucket)
	assert.Equal(t, policyJSON, decoded.PolicyJSON)
}

func TestEncodeDecodeCommand_DeleteBucketPolicy(t *testing.T) {
	orig := DeleteBucketPolicyCmd{Bucket: "policy-bucket"}

	encoded, err := EncodeCommand(CmdDeleteBucketPolicy, orig)
	require.NoError(t, err)

	cmd, err := DecodeCommand(encoded)
	require.NoError(t, err)
	assert.Equal(t, CmdDeleteBucketPolicy, cmd.Type)

	decoded, err := decodeDeleteBucketPolicyCmd(cmd.Data)
	require.NoError(t, err)
	assert.Equal(t, "policy-bucket", decoded.Bucket)
}

func TestDecodeCommands_InvalidData(t *testing.T) {
	// FlatBuffers panics on malformed data; test that the top-level entry
	// points (DecodeCommand, unmarshalSnapshotState) convert panics to errors.
	// Inner decode functions are only called with already-validated data.
	_, err := DecodeCommand([]byte("not valid flatbuffer data"))
	assert.Error(t, err, "DecodeCommand should fail on invalid data")

	_, err = unmarshalSnapshotState([]byte("not valid flatbuffer data"))
	assert.Error(t, err, "unmarshalSnapshotState should fail on invalid data")

	_, err = DecodeCommand(nil)
	assert.Error(t, err, "DecodeCommand should fail on nil data")

	_, err = unmarshalSnapshotState(nil)
	assert.Error(t, err, "unmarshalSnapshotState should fail on nil data")
}

func TestEncodePayload_UnknownCommandType(t *testing.T) {
	_, err := encodePayload(CommandType(99), CreateBucketCmd{Bucket: "x"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown command type")
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

func TestCodec_SetBucketVersioningCmd_RoundTrip(t *testing.T) {
	cmd := SetBucketVersioningCmd{Bucket: "mybucket", State: "Enabled"}
	raw, err := encodeSetBucketVersioningCmd(cmd)
	require.NoError(t, err)
	got, err := decodeSetBucketVersioningCmd(raw)
	require.NoError(t, err)
	assert.Equal(t, cmd, got)
}

func TestCodec_SetObjectACLCmd_RoundTrip(t *testing.T) {
	cmd := SetObjectACLCmd{Bucket: "b", Key: "file.txt", ACL: 2}
	raw, err := encodeSetObjectACLCmd(cmd)
	require.NoError(t, err)
	got, err := decodeSetObjectACLCmd(raw)
	require.NoError(t, err)
	assert.Equal(t, cmd, got)
}

func TestCodec_ObjectMeta_ACL_RoundTrip(t *testing.T) {
	m := objectMeta{Key: "f", Size: 5, ContentType: "text/plain", ETag: "e", LastModified: 1, ACL: 2}
	raw, err := marshalObjectMeta(m)
	require.NoError(t, err)
	got, err := unmarshalObjectMeta(raw)
	require.NoError(t, err)
	assert.Equal(t, m, got)
}

func TestCodec_ObjectMeta_ACL_BackwardCompat(t *testing.T) {
	// Old record without ACL field (ACL=0) should unmarshal cleanly as ACL=0 (private).
	m := objectMeta{Key: "f", Size: 5, ContentType: "text/plain", ETag: "e", LastModified: 1}
	raw, err := marshalObjectMeta(m)
	require.NoError(t, err)
	got, err := unmarshalObjectMeta(raw)
	require.NoError(t, err)
	assert.Equal(t, uint8(0), got.ACL, "legacy records should have ACL=0 (private)")
}

func TestCodec_SetBucketECPolicyCmd_RoundTrip(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		cmd := SetBucketECPolicyCmd{Bucket: "my-bucket", Enabled: enabled}
		raw, err := encodeSetBucketECPolicyCmd(cmd)
		require.NoError(t, err)
		got, err := decodeSetBucketECPolicyCmd(raw)
		require.NoError(t, err)
		assert.Equal(t, cmd, got)
	}
}
