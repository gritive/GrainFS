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
	// A bare PutObjectMetaCmd FB decoded as a Command must not yield the retired
	// object slot 3 (formerly CmdPutObjectMeta) carrying a re-decodable payload.
	if env, derr := DecodeCommand(blob); derr == nil {
		require.NotEqual(t, CommandType(3), env.Type, "blob must not be a retired-object-slot-tagged Command envelope")
	}
}

func TestCommandTypeWireValuesStable(t *testing.T) {
	// Live control-plane commands persist in the raft log; their values are a
	// wire contract and must never shift when retired slots are removed.
	require.Equal(t, CommandType(0), CmdNoOp)
	require.Equal(t, CommandType(1), CmdCreateBucket)
	require.Equal(t, CommandType(2), CmdDeleteBucket)
	require.Equal(t, CommandType(8), CmdSetBucketPolicy)
	require.Equal(t, CommandType(9), CmdDeleteBucketPolicy)
	require.Equal(t, CommandType(10), CmdMigrateShard)
	require.Equal(t, CommandType(11), CmdMigrationDone)
	require.Equal(t, CommandType(15), CmdSetBucketVersioning)
	require.Equal(t, CommandType(17), CmdSetRing)
	require.Equal(t, CommandType(41), CmdResealFSMValues)
	require.Equal(t, CommandType(42), CmdFSMValueResealDone)
}
