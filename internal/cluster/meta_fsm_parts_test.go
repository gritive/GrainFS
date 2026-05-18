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
