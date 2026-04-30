package cluster

import (
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPickVoters_Deterministic(t *testing.T) {
	nodes := []string{"node-0", "node-1", "node-2", "node-3", "node-4"}
	a := pickVoters("group-3", nodes, 3)
	b := pickVoters("group-3", nodes, 3)
	require.Equal(t, a, b, "same groupID + same nodes must yield same voter set")
	require.Len(t, a, 3)
}

func TestPickVoters_DifferentGroupIDsDiffer(t *testing.T) {
	// Two groups CAN map to the same top-3 voters by chance (sorted output).
	// Across 16 groups, at least one pair must differ — otherwise rendezvous
	// hashing is broken.
	nodes := []string{"node-0", "node-1", "node-2", "node-3", "node-4"}
	seen := make(map[string]bool)
	for i := 0; i < 16; i++ {
		gid := strings.Repeat("g", i+1)
		seen[strings.Join(pickVoters(gid, nodes, 3), ",")] = true
	}
	require.Greater(t, len(seen), 1, "expected at least 2 distinct voter sets across 16 groups, got %d", len(seen))
}

func TestPickVoters_RFLargerThanCluster_Clamps(t *testing.T) {
	nodes := []string{"node-0", "node-1"}
	got := pickVoters("group-0", nodes, 5)
	require.Len(t, got, 2, "RF must clamp to cluster size")
}

func TestPickVoters_SingleNode(t *testing.T) {
	nodes := []string{"only"}
	got := pickVoters("group-0", nodes, 3)
	require.Equal(t, []string{"only"}, got)
}

func TestPickVoters_EvenSpread(t *testing.T) {
	nodes := []string{"n0", "n1", "n2", "n3", "n4"}
	count := make(map[string]int)
	groups := 100
	for i := 0; i < groups; i++ {
		gid := "group-" + string(rune('A'+i%26)) + string(rune('0'+i/26))
		voters := pickVoters(gid, nodes, 3)
		for _, v := range voters {
			count[v]++
		}
	}
	// 각 노드는 100*3/5 = 60회, ±20 허용
	for _, n := range nodes {
		assert.GreaterOrEqual(t, count[n], 40, "node %s underused: %d", n, count[n])
		assert.LessOrEqual(t, count[n], 80, "node %s overused: %d", n, count[n])
	}
}

func TestPickVoters_ReturnSorted(t *testing.T) {
	nodes := []string{"node-0", "node-1", "node-2", "node-3", "node-4"}
	got := pickVoters("group-7", nodes, 3)
	sorted := make([]string, len(got))
	copy(sorted, got)
	sort.Strings(sorted)
	require.Equal(t, sorted, got, "result must be sorted (deterministic across processes)")
}

func TestPickVoters_EmptyNodes(t *testing.T) {
	got := pickVoters("group-0", nil, 3)
	require.Empty(t, got)
}
