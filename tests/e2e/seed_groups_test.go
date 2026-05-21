package e2e

import (
	"fmt"

	"github.com/onsi/ginkgo/v2"
	"github.com/stretchr/testify/require"
)

var _ = ginkgo.Describe("Seed groups", func() {
	ginkgo.Context("MRCluster3Node", func() {
		const numNodes = 3
		var groupDirs map[string]int

		ginkgo.BeforeEach(func() {
			c := startStaticMRCluster(ginkgo.GinkgoTB(), numNodes)
			groupDirs = countGroupDirsAcrossNodes(c)
		})

		runSeedGroupsAutoCases(func() map[string]int { return groupDirs })
	})
})

func runSeedGroupsAutoCases(getGroupDirs func() map[string]int) {
	ginkgo.It("creates auto-seeded group directories", func() {
		t := ginkgo.GinkgoTB()
		const numNodes = 3
		wantSeedGroups := numNodes * 4
		groupDirs := getGroupDirs()
		// group-0 is legacy metadata compatibility; normal object placement uses
		// group-1..N-1, so verify the auto-seeded normal group headroom.
		for i := 1; i < wantSeedGroups; i++ {
			gid := fmt.Sprintf("group-%d", i)
			require.NotZero(t, groupDirs[gid], "automatic seed group %s must exist", gid)
		}
		t.Logf("auto seed-groups test passed: %d groups seeded across %d nodes", wantSeedGroups, numNodes)
	})
}
