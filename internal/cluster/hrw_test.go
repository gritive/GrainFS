package cluster

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlaceShards_Deterministic(t *testing.T) {
	nodes := []string{"n1", "n2", "n3", "n4", "n5"}

	t.Run("nil weights — same input twice", func(t *testing.T) {
		a := PlaceShards("bucket/object/key", nodes, nil, 3)
		b := PlaceShards("bucket/object/key", nodes, nil, 3)
		assert.Equal(t, a, b)
		assert.Len(t, a, 3)
	})

	t.Run("explicit weights — same input twice", func(t *testing.T) {
		w := []float64{1, 1, 1, 1, 1}
		a := PlaceShards("bucket/object/key", nodes, w, 3)
		b := PlaceShards("bucket/object/key", nodes, w, 3)
		assert.Equal(t, a, b)
	})
}

func TestPlaceShards_NoDuplicates(t *testing.T) {
	nodes := []string{"n1", "n2", "n3", "n4", "n5", "n6"}
	got := PlaceShards("bucket/key", nodes, nil, 6)
	require.Len(t, got, 6)
	seen := map[string]bool{}
	for _, n := range got {
		assert.False(t, seen[n], "duplicate node: %s", n)
		seen[n] = true
	}
}

func TestPlaceShards_NodeOrder(t *testing.T) {
	nodes := []string{"n1", "n2", "n3", "n4", "n5"}
	reversed := []string{"n5", "n4", "n3", "n2", "n1"}
	a := PlaceShards("bucket/key", nodes, nil, 3)
	b := PlaceShards("bucket/key", reversed, nil, 3)
	assert.Equal(t, a, b, "node order must not affect placement")
}

func TestPlaceShards_CountClamping(t *testing.T) {
	nodes := []string{"n1", "n2"}
	got := PlaceShards("k", nodes, nil, 5)
	assert.Len(t, got, 2, "count must clamp to available node count")
}

func TestPlaceShards_EmptyNodes(t *testing.T) {
	got := PlaceShards("k", nil, nil, 3)
	assert.Nil(t, got)
}
