package e2e

import (
	"os/exec"
	"path/filepath"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// TestClusterDrainFollowerE2E spins up a 3-node cluster and drains a
// follower (no transfer needed). Verifies the voter set shrinks and the
// drained node is no longer listed.
var _ = ginkgo.Describe("Cluster drain", func() {
	ginkgo.Context("Cluster3Node", func() {
		var c *e2eCluster

		ginkgo.BeforeEach(func() {
			c = startE2ECluster(ginkgo.GinkgoTB(), e2eClusterOptions{
				Nodes:      3,
				Mode:       ClusterModeDynamicJoin,
				ClusterKey: "E2E-DRAIN-KEY",
				AccessKey:  "drain-ak",
				SecretKey:  "drain-sk",
				LogPrefix:  "grainfs-drain",
			})
		})

		ginkgo.It("drains a follower and shrinks the voter set", func() {
			t := ginkgo.GinkgoTB()
			leaderIdx := c.leaderIdx
			gomega.Expect(leaderIdx).To(gomega.BeNumerically(">=", 0), "harness must have identified leader")

			// Pick a follower.
			followerIdx := -1
			for i := range c.procs {
				if i != leaderIdx {
					followerIdx = i
					break
				}
			}
			gomega.Expect(followerIdx).To(gomega.BeNumerically(">=", 0))
			// Since PR-D, /api/cluster/status.peers reports node IDs (n1/n2/...).
			followerID := c.nodeID(followerIdx)
			leaderURL := c.httpURLs[leaderIdx]

			// Wait for 3-node membership to settle. Dynamic-join can take time.
			gomega.Eventually(func() bool {
				s := getStatusJSON(t, leaderURL)
				voters := stringList(s["peers"])
				return len(voters) == 2 && containsString(voters, followerID)
			}, 90*time.Second, 500*time.Millisecond).Should(gomega.BeTrue(), "cluster must settle with follower as a voter")

			beforeDrain := getStatusJSON(t, leaderURL)
			binary := getBinary()
			leaderSock := filepath.Join(c.dataDirs[leaderIdx], "admin.sock")
			out, err := exec.Command(binary, "cluster",
				"--endpoint", leaderSock,
				"drain", followerID, "--yes", "--timeout", "30s",
			).CombinedOutput()
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "drain follower must succeed; status=%+v out=%s", beforeDrain, out)

			output := string(out)
			gomega.Expect(output).To(gomega.ContainSubstring("is leader: false"), "follower drain must not transfer")
			gomega.Expect(output).To(gomega.ContainSubstring("drained"))

			// Verify voter set shrunk.
			gomega.Eventually(func() bool {
				s := getStatusJSON(t, leaderURL)
				voters := stringList(s["peers"])
				return len(voters) == 1 && !containsString(voters, followerID)
			}, 30*time.Second, 500*time.Millisecond).Should(gomega.BeTrue(), "voter set must shrink without follower")
		})
	})
})
