package cluster

import (
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
)

func TestCloneObjectIndexEntry_DeepCopiesParts(t *testing.T) {
	src := ObjectIndexEntry{
		Bucket: "b", Key: "k", VersionID: "v", PlacementGroupID: "g",
		Parts: []storage.MultipartPartEntry{
			{PartNumber: 1, Size: 5, ETag: "a"},
			{PartNumber: 2, Size: 7, ETag: "b"},
		},
	}
	cloned := cloneObjectIndexEntry(src)
	cloned.Parts[0].ETag = "mutated"
	require.Equal(t, "a", src.Parts[0].ETag, "clone must not alias source slice")
	require.Equal(t, "mutated", cloned.Parts[0].ETag)
}

func TestApplyPutObjectMeta_PreservesPartsOnDisk(t *testing.T) {
	cmd := PutObjectMetaCmd{
		Bucket: "b", Key: "k", VersionID: "v",
		PlacementGroupID: "g", Size: 12, ETag: "e",
		Parts: []storage.MultipartPartEntry{
			{PartNumber: 1, Size: 5, ETag: "p1"},
			{PartNumber: 2, Size: 7, ETag: "p2"},
		},
	}
	m := objectMeta{
		Key: cmd.Key, Size: cmd.Size, ETag: cmd.ETag,
		PlacementGroupID: cmd.PlacementGroupID, Parts: cmd.Parts,
	}
	raw, err := marshalObjectMeta(m)
	require.NoError(t, err)
	got, err := unmarshalObjectMeta(raw)
	require.NoError(t, err)
	require.Equal(t, cmd.Parts, got.Parts)
}

func TestBuildObjectIndexEntry_CopiesPartsFromObject(t *testing.T) {
	parts := []storage.MultipartPartEntry{
		{PartNumber: 1, Size: 5, ETag: "p1"},
		{PartNumber: 2, Size: 7, ETag: "p2"},
	}
	obj := &storage.Object{
		Size: 12, ETag: "e", Parts: parts,
	}
	entry := buildObjectIndexEntry(
		ShardGroupEntry{ID: "g", PeerIDs: []string{"n1"}},
		"b", "k", obj, false,
	)
	require.Equal(t, parts, entry.Parts)
}
