package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQuorumMetaBlobCodec_RoundTripBareFB(t *testing.T) {
	in := PutObjectMetaCmd{Bucket: "b", Key: "k", ETag: "e1", Size: 7, NodeIDs: []string{"n1"}, ECData: 1}
	blob, err := encodeQuorumMetaBlob(in)
	require.NoError(t, err)

	got, err := decodeQuorumMetaBlob(blob)
	require.NoError(t, err)
	require.Equal(t, in.Bucket, got.Bucket)
	require.Equal(t, in.Key, got.Key)
	require.Equal(t, in.ETag, got.ETag)
	require.Equal(t, in.Size, got.Size)
	require.Equal(t, in.NodeIDs, got.NodeIDs)

	// Decoupling proof: the blob is NOT a clusterpb.Command-wrapped CmdPutObjectMeta.
	// A bare PutObjectMetaCmd FB decoded as a Command must not yield a CmdPutObjectMeta
	// envelope carrying a re-decodable payload.
	if env, derr := DecodeCommand(blob); derr == nil {
		require.NotEqual(t, CmdPutObjectMeta, env.Type, "blob must not be a CmdPutObjectMeta-tagged Command envelope")
	}
}
