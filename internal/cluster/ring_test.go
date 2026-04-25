package cluster

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRing_PlacementForKey_Deterministic(t *testing.T) {
	ring := NewRing(1, []string{"node-a", "node-b", "node-c"}, 10)
	cfg := ECConfig{DataShards: 2, ParityShards: 1}

	p1 := ring.PlacementForKey(cfg, "bucket/key1")
	p2 := ring.PlacementForKey(cfg, "bucket/key1")
	assert.Equal(t, p1, p2, "same key must always map to same nodes")
}

func TestRing_PlacementForKey_NoDuplicates(t *testing.T) {
	ring := NewRing(1, []string{"node-a", "node-b", "node-c"}, 10)
	cfg := ECConfig{DataShards: 2, ParityShards: 1}

	placement := ring.PlacementForKey(cfg, "bucket/mykey")
	require.Len(t, placement, 3)
	seen := make(map[string]bool)
	for _, n := range placement {
		assert.False(t, seen[n], "duplicate node: %s", n)
		seen[n] = true
	}
}

func TestRing_PlacementForKey_Distribution(t *testing.T) {
	nodes := []string{"n1", "n2", "n3", "n4"}
	ring := NewRing(1, nodes, 150)
	cfg := ECConfig{DataShards: 2, ParityShards: 1}

	counts := make(map[string]int)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		for _, n := range ring.PlacementForKey(cfg, key) {
			counts[n]++
		}
	}
	// Each node should be picked roughly 750 times (3000 total / 4 nodes).
	// Allow 35% deviation from mean.
	mean := 3000.0 / 4
	for n, c := range counts {
		assert.InDelta(t, mean, float64(c), mean*0.35, "node %s: %d picks", n, c)
	}
}

func TestRing_WalkCW_SkipsSeen(t *testing.T) {
	ring := NewRing(1, []string{"node-a", "node-b"}, 10)
	seen := map[string]bool{"node-a": true}
	got := ring.walkCW(0, seen)
	assert.Equal(t, "node-b", got)
}
