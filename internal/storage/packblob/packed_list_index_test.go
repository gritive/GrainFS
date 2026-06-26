package packblob

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPackedListIndexPageUsesBucketPrefixAndMarker(t *testing.T) {
	var idx packedListIndex
	idx.add(packedKey{bucket: "other", key: "docs/000"})
	idx.add(packedKey{bucket: "bench", key: "img/000"})
	idx.add(packedKey{bucket: "bench", key: "docs/001"})
	idx.add(packedKey{bucket: "bench", key: "docs/003"})
	idx.add(packedKey{bucket: "bench", key: "docs/002"})

	keys, truncated := idx.page("bench", "docs/", "docs/001", 2)
	require.False(t, truncated)
	require.Equal(t, []string{"docs/002", "docs/003"}, keys)
}

func TestPackedListIndexAddRemoveIsIdempotent(t *testing.T) {
	var idx packedListIndex
	pk := packedKey{bucket: "bench", key: "docs/001"}

	idx.add(pk)
	idx.add(pk)
	idx.remove(pk)
	idx.remove(pk)

	keys, truncated := idx.page("bench", "docs/", "", 10)
	require.False(t, truncated)
	require.Empty(t, keys)
}
