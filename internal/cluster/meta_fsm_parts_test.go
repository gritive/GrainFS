package cluster

import (
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
)

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
