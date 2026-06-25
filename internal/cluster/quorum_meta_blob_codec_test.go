package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
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
	// A bare PutObjectMetaCmd FB decoded as a Command must not yield the retired
	// object slot 3 (formerly CmdPutObjectMeta) carrying a re-decodable payload.
	if derr := DecodeCommand(blob); derr == nil {
		env := clusterpb.GetRootAsCommand(blob, 0)
		require.NotEqual(t, uint32(3), env.Type(), "blob must not be a retired-object-slot-tagged Command envelope")
	}
}

func TestLegacyCommandEnvelopeTypeZeroValid(t *testing.T) {
	// Data-group command type 0 remains a valid legacy envelope type.
	raw, err := buildRawCommand(0, nil)
	require.NoError(t, err)
	require.NoError(t, DecodeCommand(raw))
	cmd := clusterpb.GetRootAsCommand(raw, 0)
	require.Equal(t, uint32(0), cmd.Type())
}
