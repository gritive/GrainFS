package e2e

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestE2E_SeedGroups_AutoFromNodeCount(t *testing.T) {

	const numNodes = 3
	wantSeedGroups := numNodes * 4

	c := startStaticMRCluster(t, numNodes)
	groupDirs := countGroupDirsAcrossNodes(c)

	// group-0 is legacy metadata compatibility; normal object placement uses
	// group-1..N-1, so verify the auto-seeded normal group headroom.
	for i := 1; i < wantSeedGroups; i++ {
		gid := fmt.Sprintf("group-%d", i)
		require.NotZero(t, groupDirs[gid], "automatic seed group %s must exist", gid)
	}
	t.Logf("auto seed-groups test passed: %d groups seeded across %d nodes", wantSeedGroups, numNodes)
}
