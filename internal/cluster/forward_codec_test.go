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
