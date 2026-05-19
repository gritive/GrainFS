package e2e

import (
	"encoding/json"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestClusterTransferLeaderE2E spins up a 3-node cluster and exercises
// `grainfs cluster transfer-leader --wait` against the leader's admin
// socket. Verifies the leader changes and term advances.
func TestClusterTransferLeaderE2E(t *testing.T) {

	c := startE2ECluster(t, e2eClusterOptions{
		Nodes:      3,
		Mode:       ClusterModeDynamicJoin,
		ClusterKey: "E2E-TRANSFER-LEADER-KEY",
		AccessKey:  "tl-ak",
		SecretKey:  "tl-sk",
		LogPrefix:  "grainfs-transfer-leader",
	})

	leaderIdx := c.leaderIdx
	require.GreaterOrEqual(t, leaderIdx, 0, "harness must have identified leader")

	// Wait for membership to settle (3-node). Dynamic-join can take time.
	leaderURL := c.httpURLs[leaderIdx]
	settled := false
	var initialTerm float64
	var initialLeader string
	for i := 0; i < 180; i++ {
		s := getStatusJSON(t, leaderURL)
		voters := stringList(s["peers"])
		if len(voters) == 2 {
			settled = true
			initialTerm, _ = s["term"].(float64)
			initialLeader, _ = s["leader_id"].(string)
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	require.True(t, settled, "cluster must settle to 3 voters")

	// Re-confirm leader index just before invoking CLI — leaderIdx may have
	// drifted between harness settle and now. Status reports node ids
	// (n1/n2/...) since PR-D.
	currentLeader := initialLeader
	currentLeaderIdx := -1
	for i := range c.procs {
		if c.nodeID(i) == currentLeader {
			currentLeaderIdx = i
			break
		}
	}
	require.GreaterOrEqual(t, currentLeaderIdx, 0, "must locate current leader by node id")

	binary := getBinary()
	sock := filepath.Join(c.dataDirs[currentLeaderIdx], "admin.sock")
	out, err := exec.Command(binary, "cluster",
		"--endpoint", sock,
		"transfer-leader", "--wait", "--timeout", "30s",
	).CombinedOutput()
	require.NoError(t, err, "transfer-leader command must succeed; out=%s", out)

	// Verify leader changed and term advanced.
	require.Eventually(t, func() bool {
		s := getStatusJSON(t, leaderURL)
		newLeader, _ := s["leader_id"].(string)
		newTerm, _ := s["term"].(float64)
		return newLeader != "" && newLeader != initialLeader && newTerm > initialTerm
	}, 30*time.Second, 500*time.Millisecond, "leader must change and term must advance")

	output := string(out)
	assert.Contains(t, output, "old_leader")
	assert.Contains(t, output, "new leader")
}

// TestClusterTransferLeaderNoPeersE2E exercises the single-node 503 path.
// The shared test server in testServerDataDir is single-node, so we can hit
// it directly without spinning up startE2ECluster.
func TestClusterTransferLeaderNoPeersE2E(t *testing.T) {

	binary := getBinary()
	sock := filepath.Join(testServerDataDir, "admin.sock")

	cmd := exec.Command(binary, "cluster",
		"--endpoint", sock,
		"transfer-leader",
	)
	out, err := cmd.CombinedOutput()
	// Either 503 (single-node) or 409 (not leader) — both acceptable; the
	// CLI exits non-zero with a TransferLeaderError-derived message.
	require.Error(t, err, "single-node transfer-leader must fail; out=%s", out)
	output := string(out)
	// Must not be a happy-path success.
	assert.NotContains(t, output, "(use --wait")

	// Sanity: server still up afterwards.
	statusOut, sErr := exec.Command(binary, "cluster", "--endpoint", sock, "status", "--format", "json").Output()
	require.NoError(t, sErr)
	var s map[string]any
	require.NoError(t, json.Unmarshal(statusOut, &s))
	require.NotNil(t, s["mode"])
}
