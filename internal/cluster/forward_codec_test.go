package cluster

import (
	"testing"

	"github.com/gritive/GrainFS/internal/raft/raftpb"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
)

func TestForwardCodec_ListPartsArgs(t *testing.T) {
	args := buildListPartsArgs("bucket", "key", "upload-1", 500)

	decoded := raftpb.GetRootAsListPartsArgs(args, 0)
	require.Equal(t, "bucket", string(decoded.Bucket()))
	require.Equal(t, "key", string(decoded.Key()))
	require.Equal(t, "upload-1", string(decoded.UploadId()))
	require.Equal(t, int32(500), decoded.MaxParts())
}

func TestForwardCodec_ForwardReplyParts(t *testing.T) {
	reply := buildPartsReply([]storage.Part{
		{PartNumber: 1, ETag: "etag-1", Size: 10},
		{PartNumber: 2, ETag: "etag-2", Size: 20},
	})

	parts, err := partsFromReply(reply)
	require.NoError(t, err)
	require.Equal(t, []storage.Part{
		{PartNumber: 1, ETag: "etag-1", Size: 10},
		{PartNumber: 2, ETag: "etag-2", Size: 20},
	}, parts)
}

func TestForwardCodec_ListMultipartUploadsArgs(t *testing.T) {
	args := buildListMultipartUploadsArgs("bucket", "prefix/", 500)

	decoded := raftpb.GetRootAsListMultipartUploadsArgs(args, 0)
	require.Equal(t, "bucket", string(decoded.Bucket()))
	require.Equal(t, "prefix/", string(decoded.Prefix()))
	require.Equal(t, int32(500), decoded.MaxUploads())
}

func TestForwardCodec_ForwardReplyMultipartUploads(t *testing.T) {
	reply := buildMultipartUploadsReply([]*storage.MultipartUpload{
		{Bucket: "bucket", Key: "prefix/a.bin", UploadID: "upload-1", ContentType: "text/plain", CreatedAt: 11},
		{Bucket: "bucket", Key: "prefix/b.bin", UploadID: "upload-2", ContentType: "application/octet-stream", CreatedAt: 22},
	})

	uploads, err := multipartUploadsFromReply(reply)
	require.NoError(t, err)
	require.Equal(t, []*storage.MultipartUpload{
		{Bucket: "bucket", Key: "prefix/a.bin", UploadID: "upload-1", ContentType: "text/plain", CreatedAt: 11},
		{Bucket: "bucket", Key: "prefix/b.bin", UploadID: "upload-2", ContentType: "application/octet-stream", CreatedAt: 22},
	}, uploads)
}

func TestForwardStatusAppendObjectTooLargeRoundTrip(t *testing.T) {
	require.Equal(t, raftpb.ForwardStatusAppendObjectTooLarge,
		mapErrorToStatus(storage.ErrAppendObjectTooLarge))
}
