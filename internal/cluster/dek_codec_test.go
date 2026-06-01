package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

// TestDEKRewrapProgressCmd_EpochRoundTrip verifies that the epoch field
// survives encode→decode on the progress cmd.
func TestDEKRewrapProgressCmd_EpochRoundTrip(t *testing.T) {
	data, err := encodeMetaDEKRewrapProgressCmd("node-3", 7, 1)
	require.NoError(t, err)
	nodeID, gen, epoch, err := decodeMetaDEKRewrapProgressCmd(data)
	require.NoError(t, err)
	require.Equal(t, "node-3", nodeID)
	require.Equal(t, uint32(7), gen)
	require.Equal(t, uint32(1), epoch, "epoch must round-trip")
}

// TestDEKRewrapProgressCmd_LegacyNoEpoch verifies that a flatbuffer built
// without the epoch field (old wire format) decodes epoch as 0.
func TestDEKRewrapProgressCmd_LegacyNoEpoch(t *testing.T) {
	// Build a legacy 2-field MetaDEKRewrapProgressCmd without epoch.
	b := clusterBuilderPool.Get()
	nid := b.CreateString("node-old")
	clusterpb.MetaDEKRewrapProgressCmdStart(b)
	clusterpb.MetaDEKRewrapProgressCmdAddNodeId(b, nid)
	clusterpb.MetaDEKRewrapProgressCmdAddGen(b, 5)
	off := clusterpb.MetaDEKRewrapProgressCmdEnd(b)
	buf := fbFinish(b, off)

	nodeID, gen, epoch, err := decodeMetaDEKRewrapProgressCmd(buf)
	require.NoError(t, err)
	require.Equal(t, "node-old", nodeID)
	require.Equal(t, uint32(5), gen)
	require.Equal(t, uint32(0), epoch, "absent epoch field must decode as 0")
}

// TestDEKVersionSnapshot_EpochRoundTrip verifies that the node_epochs vector
// in DEKRewrapDoneEntry survives encode→decode, and that legacy entries
// (absent node_epochs) decode to epoch 0 for every node.
func TestDEKVersionSnapshot_EpochRoundTrip(t *testing.T) {
	versions := map[uint32][]byte{2: make([]byte, 60)}
	rewrapDone := map[uint32]map[string]uint32{
		2: {"node-a": 1, "node-b": 0},
	}
	payload, err := encodeMetaDEKVersionSnapshot(versions, 2, nil, 0, rewrapDone)
	require.NoError(t, err)

	_, _, _, _, got, err := decodeMetaDEKVersionSnapshot(payload)
	require.NoError(t, err)
	require.Equal(t, uint32(1), got[2]["node-a"], "node-a epoch must be 1")
	require.Equal(t, uint32(0), got[2]["node-b"], "node-b epoch must be 0")
}

// TestDEKVersionSnapshot_LegacyNoNodeEpochs verifies backward compat:
// a snapshot encoded without node_epochs decodes epoch 0 for every node.
func TestDEKVersionSnapshot_LegacyNoNodeEpochs(t *testing.T) {
	// Build a legacy DEKRewrapDoneEntry with only gen+node_ids (no node_epochs).
	versions := map[uint32][]byte{3: make([]byte, 60)}
	// Use the OLD signature by building the snapshot with a struct{} rewrapDone
	// manually via flatbuffers to mimic a pre-epoch wire format.
	b := clusterBuilderPool.Get()

	// Build node_ids vector.
	nodeStr := b.CreateString("node-x")
	clusterpb.DEKRewrapDoneEntryStartNodeIdsVector(b, 1)
	b.PrependUOffsetT(nodeStr)
	nodeVec := b.EndVector(1)

	clusterpb.DEKRewrapDoneEntryStart(b)
	clusterpb.DEKRewrapDoneEntryAddGen(b, 3)
	clusterpb.DEKRewrapDoneEntryAddNodeIds(b, nodeVec)
	doneEntry := clusterpb.DEKRewrapDoneEntryEnd(b)

	// Build version entry.
	wrappedOff := b.CreateByteVector(versions[3])
	clusterpb.DEKVersionEntryStart(b)
	clusterpb.DEKVersionEntryAddGen(b, 3)
	clusterpb.DEKVersionEntryAddWrapped(b, wrappedOff)
	verEntry := clusterpb.DEKVersionEntryEnd(b)

	clusterpb.MetaDEKVersionSnapshotStartVersionsVector(b, 1)
	b.PrependUOffsetT(verEntry)
	versVec := b.EndVector(1)

	clusterpb.MetaDEKVersionSnapshotStartRewrapDoneVector(b, 1)
	b.PrependUOffsetT(doneEntry)
	rewrapDoneVec := b.EndVector(1)

	clusterpb.MetaDEKVersionSnapshotStart(b)
	clusterpb.MetaDEKVersionSnapshotAddVersions(b, versVec)
	clusterpb.MetaDEKVersionSnapshotAddActive(b, 3)
	clusterpb.MetaDEKVersionSnapshotAddRewrapDone(b, rewrapDoneVec)
	root := clusterpb.MetaDEKVersionSnapshotEnd(b)
	buf := fbFinish(b, root)

	_, _, _, _, got, err := decodeMetaDEKVersionSnapshot(buf)
	require.NoError(t, err)
	nodeEpoch, ok := got[3]["node-x"]
	require.True(t, ok, "node-x must appear in done set")
	require.Equal(t, uint32(0), nodeEpoch, "absent node_epochs must default to 0")
}
