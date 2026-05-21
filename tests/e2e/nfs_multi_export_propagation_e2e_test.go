package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/stretchr/testify/require"
)

var _ = ginkgo.Describe("NFS multi-export propagation", func() {
	ginkgo.Context("MRCluster3Node", func() {
		var c *mrCluster

		ginkgo.BeforeEach(func() {
			c = startMRCluster(ginkgo.GinkgoTB(), 3, mrClusterOptions{
				disableNFS4:   true,
				disableNBD:    true,
				FastBootstrap: true,
			})
		})

		runNFSMultiExportPropagationCases(func() *mrCluster { return c })
	})
})

func runNFSMultiExportPropagationCases(getCluster func() *mrCluster) {
	ginkgo.It("propagates an admin-added export to all nodes", func() {
		t := ginkgo.GinkgoTB()
		c := getCluster()
		bucket := fmt.Sprintf("nfs-prop-e2e-%d", freePort())
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		ginkgo.DeferCleanup(cancel)
		requireMRCreateBucketEventually(t, ctx, c, bucket)

		adminNode := (c.leaderIdx + 1) % c.nodeCount
		created := runNfsExportJSONOnDataDir(t, c.dataDirs[adminNode], "add", bucket)
		require.Equal(t, bucket, created.Bucket)
		require.NotZero(t, created.Generation)

		for i := 0; i < c.nodeCount; i++ {
			dataDir := c.dataDirs[i]
			require.Eventually(t, func() bool {
				return jsonExportListContains(t, dataDir, bucket, created.Generation)
			}, 10*time.Second, 100*time.Millisecond, "node %d did not observe export", i)
		}
	})
}

func runNfsExportJSONOnDataDir(t testing.TB, dataDir, verb, bucket string, flags ...string) e2eNfsExport {
	t.Helper()
	args := []string{"nfs", "export", verb, bucket, "--json"}
	args = append(args, flags...)
	var out string
	require.Eventually(t, func() bool {
		var code int
		out, code = runCLI(t, dataDir, args...)
		return code == 0
	}, 45*time.Second, 500*time.Millisecond, "%s", out)
	return parseSingleNfsExport(t, out)
}

func jsonExportListContains(t testing.TB, dataDir, bucket string, minGeneration uint64) bool {
	t.Helper()
	out, code := runCLI(t, dataDir, "nfs", "export", "list", "--json")
	if code != 0 {
		return false
	}
	for _, row := range parseNfsExportList(t, out) {
		if row.Bucket == bucket && row.Generation >= minGeneration {
			return true
		}
	}
	return false
}
