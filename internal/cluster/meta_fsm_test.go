package cluster

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeAddNodeCmd(t *testing.T, id, addr string, role uint8) []byte {
	t.Helper()
	data, err := encodeMetaAddNodeCmd(MetaNodeEntry{ID: id, Address: addr, Role: role})
	require.NoError(t, err)
	cmd, err := encodeMetaCmd(MetaCmdTypeAddNode, data)
	require.NoError(t, err)
	return cmd
}

func makeRemoveNodeCmd(t *testing.T, id string) []byte {
	t.Helper()
	data, err := encodeMetaRemoveNodeCmd(id)
	require.NoError(t, err)
	cmd, err := encodeMetaCmd(MetaCmdTypeRemoveNode, data)
	require.NoError(t, err)
	return cmd
}

func TestMetaFSM_Apply_AddNode(t *testing.T) {
	f := NewMetaFSM()
	err := f.applyCmd(makeAddNodeCmd(t, "node-1", "10.0.0.1:7001", 0))
	require.NoError(t, err)

	nodes := f.Nodes()
	require.Len(t, nodes, 1)
	assert.Equal(t, "node-1", nodes[0].ID)
	assert.Equal(t, "10.0.0.1:7001", nodes[0].Address)
	assert.Equal(t, uint8(0), nodes[0].Role)
}

func TestMetaFSM_Apply_RemoveNode(t *testing.T) {
	f := NewMetaFSM()
	require.NoError(t, f.applyCmd(makeAddNodeCmd(t, "node-1", "10.0.0.1:7001", 0)))
	require.NoError(t, f.applyCmd(makeAddNodeCmd(t, "node-2", "10.0.0.2:7001", 0)))

	require.NoError(t, f.applyCmd(makeRemoveNodeCmd(t, "node-1")))

	nodes := f.Nodes()
	require.Len(t, nodes, 1)
	assert.Equal(t, "node-2", nodes[0].ID)
}

func TestMetaFSM_Apply_NoOp(t *testing.T) {
	f := NewMetaFSM()
	require.NoError(t, f.applyCmd(makeAddNodeCmd(t, "node-1", "10.0.0.1:7001", 0)))

	noopCmd, err := encodeMetaCmd(MetaCmdTypeNoOp, nil)
	require.NoError(t, err)
	require.NoError(t, f.applyCmd(noopCmd))

	assert.Len(t, f.Nodes(), 1, "NoOp must not change state")
}

func TestMetaFSM_Snapshot_Restore(t *testing.T) {
	f := NewMetaFSM()
	require.NoError(t, f.applyCmd(makeAddNodeCmd(t, "node-1", "addr-1:7001", 0)))
	require.NoError(t, f.applyCmd(makeAddNodeCmd(t, "node-2", "addr-2:7001", 1)))

	snap, err := f.Snapshot()
	require.NoError(t, err)
	require.NotEmpty(t, snap)

	f2 := NewMetaFSM()
	require.NoError(t, f2.Restore(snap))

	nodes := f2.Nodes()
	require.Len(t, nodes, 2)
	ids := map[string]bool{}
	for _, n := range nodes {
		ids[n.ID] = true
	}
	assert.True(t, ids["node-1"])
	assert.True(t, ids["node-2"])
}

func makePutShardGroupCmd(t *testing.T, id string, peers []string) []byte {
	t.Helper()
	data, err := encodeMetaPutShardGroupCmd(ShardGroupEntry{ID: id, PeerIDs: peers})
	require.NoError(t, err)
	cmd, err := encodeMetaCmd(MetaCmdTypePutShardGroup, data)
	require.NoError(t, err)
	return cmd
}

func TestMetaFSM_Apply_PutShardGroup(t *testing.T) {
	f := NewMetaFSM()
	err := f.applyCmd(makePutShardGroupCmd(t, "group-0", []string{"node-0", "node-1"}))
	require.NoError(t, err)

	groups := f.ShardGroups()
	require.Len(t, groups, 1)
	assert.Equal(t, "group-0", groups[0].ID)
	assert.Equal(t, []string{"node-0", "node-1"}, groups[0].PeerIDs)
}

func TestMetaFSM_ShardGroups_Snapshot_Restore(t *testing.T) {
	f := NewMetaFSM()
	require.NoError(t, f.applyCmd(makeAddNodeCmd(t, "node-0", "10.0.0.1:7001", 0)))
	require.NoError(t, f.applyCmd(makePutShardGroupCmd(t, "group-0", []string{"node-0"})))

	snap, err := f.Snapshot()
	require.NoError(t, err)
	require.NotEmpty(t, snap)

	f2 := NewMetaFSM()
	require.NoError(t, f2.Restore(snap))

	assert.Len(t, f2.Nodes(), 1)

	groups := f2.ShardGroups()
	require.Len(t, groups, 1)
	assert.Equal(t, "group-0", groups[0].ID)
	assert.Equal(t, []string{"node-0"}, groups[0].PeerIDs)
}

func TestMetaFSM_Apply_UnknownType_Noop(t *testing.T) {
	f := NewMetaFSM()
	// MetaCmdType 255 is unknown — must not panic
	unknownCmd, err := encodeMetaCmd(255, nil)
	require.NoError(t, err)
	require.NoError(t, f.applyCmd(unknownCmd))
	assert.Empty(t, f.Nodes())
}

func makePutBucketAssignmentCmd(t *testing.T, bucket, groupID string) []byte {
	t.Helper()
	data, err := encodeMetaPutBucketAssignmentCmd(bucket, groupID)
	require.NoError(t, err)
	cmd, err := encodeMetaCmd(MetaCmdTypePutBucketAssignment, data)
	require.NoError(t, err)
	return cmd
}

func TestMetaFSM_Apply_PutBucketAssignment(t *testing.T) {
	f := NewMetaFSM()
	err := f.applyCmd(makePutBucketAssignmentCmd(t, "photos", "group-0"))
	require.NoError(t, err)

	assignments := f.BucketAssignments()
	require.Len(t, assignments, 1)
	assert.Equal(t, "group-0", assignments["photos"])
}

func TestMetaFSM_Apply_PutBucketAssignment_Overwrite(t *testing.T) {
	f := NewMetaFSM()
	require.NoError(t, f.applyCmd(makePutBucketAssignmentCmd(t, "photos", "group-0")))
	require.NoError(t, f.applyCmd(makePutBucketAssignmentCmd(t, "photos", "group-1")))

	assignments := f.BucketAssignments()
	require.Len(t, assignments, 1)
	assert.Equal(t, "group-1", assignments["photos"])
}

func TestMetaFSM_BucketAssignments_Snapshot_Restore(t *testing.T) {
	f := NewMetaFSM()
	require.NoError(t, f.applyCmd(makePutBucketAssignmentCmd(t, "photos", "group-0")))
	require.NoError(t, f.applyCmd(makePutBucketAssignmentCmd(t, "videos", "group-1")))

	snap, err := f.Snapshot()
	require.NoError(t, err)
	require.NotEmpty(t, snap)

	f2 := NewMetaFSM()
	require.NoError(t, f2.Restore(snap))

	assignments := f2.BucketAssignments()
	require.Len(t, assignments, 2)
	assert.Equal(t, "group-0", assignments["photos"])
	assert.Equal(t, "group-1", assignments["videos"])
}

func TestMetaFSM_OnBucketAssigned_CallbackFired(t *testing.T) {
	f := NewMetaFSM()
	var cbBucket, cbGroup string
	f.SetOnBucketAssigned(func(bucket, groupID string) {
		cbBucket = bucket
		cbGroup = groupID
	})
	require.NoError(t, f.applyCmd(makePutBucketAssignmentCmd(t, "photos", "group-0")))
	assert.Equal(t, "photos", cbBucket)
	assert.Equal(t, "group-0", cbGroup)
}
