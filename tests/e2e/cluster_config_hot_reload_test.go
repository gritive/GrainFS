package e2e

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// TestClusterConfigHotReloadFollowerObservesE2E validates the
// divergence-impossible guarantee end-to-end: PATCH at the leader's admin UDS,
// then observe the new value on a *follower* node's admin UDS. Reads come from
// each node's local MetaFSM (Raft-replicated), so a successful PATCH at the
// leader must eventually propagate to every follower with no per-node
// override possible.
var _ = ginkgo.Describe("Cluster config hot reload", func() {
	ginkgo.Context("Cluster3Node", func() {
		var c *e2eCluster

		ginkgo.BeforeEach(func() {
			c = startE2ECluster(ginkgo.GinkgoTB(), e2eClusterOptions{
				Nodes:      3,
				Mode:       ClusterModeStaticPeers,
				LogPrefix:  "grainfs-clusterconfig-hotreload",
				DisableNFS: true,
				DisableNBD: true,
			})
		})

		ginkgo.It("propagates leader-applied config changes to followers", func() {
			t := ginkgo.GinkgoTB()
			leaderDir := c.dataDirs[c.leaderIdx]
			// Pick any non-leader as the follower probe target.
			followerIdx := (c.leaderIdx + 1) % len(c.dataDirs)
			followerDir := c.dataDirs[followerIdx]

			const newWarn = 0.50
			rev := SetClusterConfig(t, leaderDir, map[string]any{
				"disk-warn-threshold": newWarn,
			})
			gomega.Expect(rev).To(gomega.BeNumerically(">", 0), "PATCH must return a non-zero rev")

			// Follower's GET /v1/cluster/config eventually shows the new value. JSON
			// numbers decode as float64 in map[string]any.
			gomega.Eventually(func() bool {
				got := GetClusterConfig(t, followerDir)
				if got.Rev < rev {
					return false
				}
				v, ok := got.Effective["disk-warn-threshold"].(float64)
				return ok && v == newWarn
			}, 10*time.Second, 200*time.Millisecond).Should(gomega.BeTrue(),
				"follower (node %d) must observe leader-applied disk-warn-threshold=%.2f",
				followerIdx, newWarn)
		})
	})
})
