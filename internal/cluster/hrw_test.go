package cluster

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
