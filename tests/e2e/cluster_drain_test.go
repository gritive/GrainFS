package e2e

import (
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestClusterDrainFollowerE2E spins up a 3-node cluster and drains a
// follower (no transfer needed). Verifies the voter set shrinks and the
// drained node is no longer listed.
func TestClusterDrainFollowerE2E(t *testing.T) {
	t.Run("Cluster3Node", func(t *testing.T) {

		c := startE2ECluster(t, e2eClusterOptions{
			Nodes:      3,
			Mode:       ClusterModeDynamicJoin,
			ClusterKey: "E2E-DRAIN-KEY",
			AccessKey:  "drain-ak",
			SecretKey:  "drain-sk",
			LogPrefix:  "grainfs-drain",
		})

		leaderIdx := c.leaderIdx
		require.GreaterOrEqual(t, leaderIdx, 0, "harness must have identified leader")

		// Pick a follower.
		followerIdx := -1
		for i := range c.procs {
			if i != leaderIdx {
				followerIdx = i
				break
			}
		}
		require.GreaterOrEqual(t, followerIdx, 0)
		// Since PR-D, /api/cluster/status.peers reports node IDs (n1/n2/...).
		followerID := c.nodeID(followerIdx)
		leaderURL := c.httpURLs[leaderIdx]

		// Wait for 3-node membership to settle. Dynamic-join can take time.
		require.Eventually(t, func() bool {
			s := getStatusJSON(t, leaderURL)
			voters := stringList(s["peers"])
			return len(voters) == 2 && containsString(voters, followerID)
		}, 90*time.Second, 500*time.Millisecond, "cluster must settle with follower as a voter")

		beforeDrain := getStatusJSON(t, leaderURL)
		binary := getBinary()
		leaderSock := filepath.Join(c.dataDirs[leaderIdx], "admin.sock")
		out, err := exec.Command(binary, "cluster",
			"--endpoint", leaderSock,
			"drain", followerID, "--yes", "--timeout", "30s",
		).CombinedOutput()
		require.NoError(t, err, "drain follower must succeed; status=%+v out=%s", beforeDrain, out)

		output := string(out)
		assert.Contains(t, output, "is leader: false", "follower drain must not transfer")
		assert.Contains(t, output, "drained")

		// Verify voter set shrunk.
		require.Eventually(t, func() bool {
			s := getStatusJSON(t, leaderURL)
			voters := stringList(s["peers"])
			return len(voters) == 1 && !containsString(voters, followerID)
		}, 30*time.Second, 500*time.Millisecond, "voter set must shrink without follower")
	})
}
