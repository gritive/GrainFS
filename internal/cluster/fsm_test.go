package cluster

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeDecodeCommand(t *testing.T) {
	tests := []struct {
		name    string
		cmdType CommandType
		payload any
	}{
		{"create_bucket", CmdCreateBucket, CreateBucketCmd{Bucket: "test"}},
		{"delete_bucket", CmdDeleteBucket, DeleteBucketCmd{Bucket: "test"}},
		{"put_object_meta", CmdPutObjectMeta, PutObjectMetaCmd{
			Bucket: "b", Key: "k", Size: 100, ContentType: "text/plain", ETag: "abc", ModTime: 12345,
		}},
		{"delete_object", CmdDeleteObject, DeleteObjectCmd{Bucket: "b", Key: "k"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := EncodeCommand(tt.cmdType, tt.payload)
			require.NoError(t, err)

			cmd, err := DecodeCommand(data)
			require.NoError(t, err)
			assert.Equal(t, tt.cmdType, cmd.Type)
			assert.NotEmpty(t, cmd.Data)
		})
	}
}

func TestDecodeCommand_Invalid(t *testing.T) {
	_, err := DecodeCommand([]byte("not valid protobuf"))
	assert.Error(t, err)
}

func TestEncodeDecodeCommand_AllTypes(t *testing.T) {
	tests := []struct {
		name    string
		cmdType CommandType
		payload any
	}{
		{"create_bucket", CmdCreateBucket, CreateBucketCmd{Bucket: "test"}},
		{"delete_bucket", CmdDeleteBucket, DeleteBucketCmd{Bucket: "test"}},
		{"put_object_meta", CmdPutObjectMeta, PutObjectMetaCmd{
			Bucket: "b", Key: "k", Size: 100, ContentType: "text/plain", ETag: "abc", ModTime: 12345,
		}},
		{"delete_object", CmdDeleteObject, DeleteObjectCmd{Bucket: "b", Key: "k"}},
		{"create_multipart", CmdCreateMultipartUpload, CreateMultipartUploadCmd{
			UploadID: "u1", Bucket: "b", Key: "k", ContentType: "binary", CreatedAt: 100,
		}},
		{"complete_multipart", CmdCompleteMultipart, CompleteMultipartCmd{
			Bucket: "b", Key: "k", UploadID: "u1", Size: 500,
			ContentType: "binary", ETag: "etag", ModTime: 200,
		}},
		{"abort_multipart", CmdAbortMultipart, AbortMultipartCmd{
			Bucket: "b", Key: "k", UploadID: "u1",
		}},
		{"set_bucket_policy", CmdSetBucketPolicy, SetBucketPolicyCmd{
			Bucket: "b", PolicyJSON: []byte(`{"policy":true}`),
		}},
		{"delete_bucket_policy", CmdDeleteBucketPolicy, DeleteBucketPolicyCmd{Bucket: "b"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := EncodeCommand(tt.cmdType, tt.payload)
			require.NoError(t, err)

			cmd, err := DecodeCommand(data)
			require.NoError(t, err)
			assert.Equal(t, tt.cmdType, cmd.Type)
			assert.NotEmpty(t, cmd.Data)
		})
	}
}
