package cluster

import (
	"fmt"
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

func TestPlaceShards_Distribution_Uniform(t *testing.T) {
	const trials = 10000
	const tolerance = 0.10 // ±10%
	nodes := []string{"n1", "n2", "n3", "n4", "n5"}

	counts := map[string]int{}
	for i := 0; i < trials; i++ {
		got := PlaceShards(fmt.Sprintf("key/%d", i), nodes, nil, 1)
		require.Len(t, got, 1)
		counts[got[0]]++
	}

	expected := float64(trials) / float64(len(nodes))
	for _, n := range nodes {
		got := float64(counts[n])
		ratio := got / expected
		assert.InDelta(t, 1.0, ratio, tolerance,
			"node %s primary count %v not within ±%v of %v", n, got, tolerance, expected)
	}
}

func TestPlaceShards_Distribution_Weighted(t *testing.T) {
	const trials = 20000
	const tolerance = 0.15 // ±15% (weighted 분산 더 큼)
	nodes := []string{"n1", "n2", "n3"}
	weights := []float64{1, 2, 4} // 합 7

	counts := map[string]int{}
	for i := 0; i < trials; i++ {
		got := PlaceShards(fmt.Sprintf("key/%d", i), nodes, weights, 1)
		require.Len(t, got, 1)
		counts[got[0]]++
	}

	totalW := 0.0
	for _, w := range weights {
		totalW += w
	}
	for i, n := range nodes {
		got := float64(counts[n])
		expected := float64(trials) * weights[i] / totalW
		ratio := got / expected
		assert.InDelta(t, 1.0, ratio, tolerance,
			"node %s (w=%v) primary count %v not within ±%v of %v", n, weights[i], got, tolerance, expected)
	}
}

func TestPlaceShards_ZeroWeightDrain(t *testing.T) {
	nodes := []string{"n1", "n2", "n3"}
	weights := []float64{0, 1, 1}
	for i := 0; i < 1000; i++ {
		got := PlaceShards(fmt.Sprintf("k/%d", i), nodes, weights, 2)
		require.Len(t, got, 2)
		for _, n := range got {
			assert.NotEqual(t, "n1", n, "zero-weight node must be excluded")
		}
	}
}

func TestPlaceShards_NilEquivalentToOnes(t *testing.T) {
	nodes := []string{"n1", "n2", "n3", "n4", "n5"}
	ones := []float64{1, 1, 1, 1, 1}
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("k/%d", i)
		a := PlaceShards(key, nodes, nil, 3)
		b := PlaceShards(key, nodes, ones, 3)
		assert.Equal(t, a, b, "nil weights must equal all-ones weights for key %s", key)
	}
}

func TestPlaceShards_NodeChurn_Uniform(t *testing.T) {
	const trials = 5000
	before := []string{"n1", "n2", "n3", "n4", "n5"}
	after := append([]string{}, before...)
	after = append(after, "n6") // 1 node added (1/6 churn 기대)

	moved := 0
	for i := 0; i < trials; i++ {
		key := fmt.Sprintf("key/%d", i)
		a := PlaceShards(key, before, nil, 1)
		b := PlaceShards(key, after, nil, 1)
		require.Len(t, a, 1)
		require.Len(t, b, 1)
		if a[0] != b[0] {
			moved++
		}
	}

	ratio := float64(moved) / float64(trials)
	expected := 1.0 / float64(len(after)) // 1/N
	assert.InDelta(t, expected, ratio, 0.05,
		"churn ratio %v not within ±0.05 of %v", ratio, expected)
}

func TestPlaceShards_NodeChurn_Weighted(t *testing.T) {
	const trials = 5000
	beforeNodes := []string{"n1", "n2", "n3"}
	beforeW := []float64{1, 1, 1}
	afterNodes := append([]string{}, beforeNodes...)
	afterNodes = append(afterNodes, "n4")
	afterW := []float64{1, 1, 1, 2} // 2 weight 노드 추가, 총 5

	moved := 0
	for i := 0; i < trials; i++ {
		key := fmt.Sprintf("k/%d", i)
		a := PlaceShards(key, beforeNodes, beforeW, 1)
		b := PlaceShards(key, afterNodes, afterW, 1)
		if len(a) > 0 && len(b) > 0 && a[0] != b[0] {
			moved++
		}
	}

	ratio := float64(moved) / float64(trials)
	expected := 2.0 / 5.0 // Δw/Σw_after
	assert.InDelta(t, expected, ratio, 0.07,
		"weighted churn ratio %v not within ±0.07 of %v", ratio, expected)
}
