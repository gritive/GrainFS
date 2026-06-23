package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestObjectAndPlacementFromCmd_AppendableDeriveFields verifies that
// objectAndPlacementFromCmd correctly materializes IsAppendable and Coalesced
// from a PutObjectMetaCmd onto the returned storage.Object.
// This is Task 1 F1: the read-derive must populate these fields so that
// a blob-resident appendable object reads back correctly.
func TestObjectAndPlacementFromCmd_AppendableDeriveFields(t *testing.T) {
	cmd := PutObjectMetaCmd{
		Bucket: "b", Key: "k", Size: 30, ModTime: 1700000000,
		VersionID: "v1",
		Segments: []SegmentMetaEntry{
			{BlobID: "s1", Size: 10, SegmentIdx: 0},
			{BlobID: "s2", Size: 20, SegmentIdx: 1},
		},
		Coalesced: []CoalescedShardRef{
			{CoalescedID: "c1", Size: 30, ETag: "etag1", ShardKey: "k/coalesced/c1"},
		},
		IsAppendable: true,
		MetaSeq:      5,
	}

	obj, _ := objectAndPlacementFromCmd(cmd)

	require.True(t, obj.IsAppendable, "storage.Object.IsAppendable must be true")
	require.Len(t, obj.Coalesced, 1)
	require.Equal(t, "c1", obj.Coalesced[0].CoalescedID)
	require.Equal(t, int64(30), obj.Coalesced[0].Size)
	require.Equal(t, "etag1", obj.Coalesced[0].ETag)
	require.Len(t, obj.Segments, 2, "Segments from cmd must be materialized")
	require.Equal(t, "s1", obj.Segments[0].BlobID)
	require.Equal(t, "s2", obj.Segments[1].BlobID)
}
