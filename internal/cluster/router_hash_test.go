package cluster

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHashAssign_Deterministic(t *testing.T) {
	groups := []string{"group-0", "group-1", "group-2", "group-3"}
	a := HashAssign("my-bucket", groups)
	b := HashAssign("my-bucket", groups)
	require.Equal(t, a, b)
	require.Contains(t, groups, a)
}

func TestHashAssign_DifferentBuckets_SpreadAcrossGroups(t *testing.T) {
	groups := []string{"group-0", "group-1", "group-2", "group-3"}
	count := make(map[string]int)
	for i := 0; i < 100; i++ {
		bucket := fmt.Sprintf("bkt-%d-%s", i, strings.Repeat("a", i%7))
		count[HashAssign(bucket, groups)]++
	}
	// 100/4 = 25 ± 15
	for _, g := range groups {
		assert.GreaterOrEqual(t, count[g], 10, "group %s underused: %d", g, count[g])
		assert.LessOrEqual(t, count[g], 40, "group %s overused: %d", g, count[g])
	}
}

func TestHashAssign_EmptyGroups_ReturnsEmpty(t *testing.T) {
	require.Equal(t, "", HashAssign("any", nil))
}

func TestHashAssign_SingleGroup(t *testing.T) {
	require.Equal(t, "g0", HashAssign("any-bucket", []string{"g0"}))
}
