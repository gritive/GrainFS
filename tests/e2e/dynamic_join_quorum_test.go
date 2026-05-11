package e2e

import (
	"encoding/json"
	"net/http"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestE2E_DynamicJoinTwoSurvivorReelect spins up a 3-node ClusterModeDynamicJoin
// cluster, SIGKILLs the leader, and asserts the two survivors elect a new
// leader. Before the config.Peers self-exclusion fix the joined nodes carried a
// phantom self-address so currentVoters had 4 strings → quorum 3 → 2 survivors
// stuck. Regression for TODOS.md "DynamicJoin quorum inflation".
func TestE2E_DynamicJoinTwoSurvivorReelect(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e")
	}
	if _, err := os.Stat(getBinary()); err != nil {
		t.Skipf("grainfs binary not found at %s — run `make build` first", getBinary())
	}

	c := startE2ECluster(t, e2eClusterOptions{
		Nodes:      3,
		Mode:       ClusterModeDynamicJoin,
		ClusterKey: "E2E-DJ-QUORUM-KEY",
		AccessKey:  "dj-ak",
		SecretKey:  "dj-sk",
		LogPrefix:  "grainfs-dj-quorum",
	})

	// Settle to 3 voters (peers excludes self, so len == 2).
	leaderURL := c.httpURLs[c.leaderIdx]
	var leaderID string
	require.Eventually(t, func() bool {
		s := getStatusJSON(t, leaderURL)
		voters := stringList(s["peers"])
		if len(voters) == 2 {
			leaderID, _ = s["leader_id"].(string)
			return leaderID != ""
		}
		return false
	}, 90*time.Second, 500*time.Millisecond, "cluster must settle to 3 voters")

	// Find and SIGKILL the leader.
	leaderIdx := -1
	for i := range c.procs {
		if c.nodeID(i) == leaderID {
			leaderIdx = i
			break
		}
	}
	require.GreaterOrEqual(t, leaderIdx, 0, "must locate the current leader by node id")
	t.Logf("killing leader node %d (%s) to trigger re-election", leaderIdx, leaderID)
	require.NoError(t, c.procs[leaderIdx].Process.Signal(syscall.SIGKILL))
	_ = c.procs[leaderIdx].Wait()
	c.procs[leaderIdx] = nil // prevent double-handling in cluster Stop()

	// Surviving node indexes (everything except the killed leader).
	var survivorIdxs []int
	for i := range c.procs {
		if i != leaderIdx {
			survivorIdxs = append(survivorIdxs, i)
		}
	}
	require.Len(t, survivorIdxs, 2, "must have exactly 2 surviving nodes")

	// Poll the survivors directly (tolerating transient HTTP errors during the
	// election) until one of them reports a *different* leader id.
	tryLeaderID := func(url string) string {
		resp, err := http.Get(url + "/api/cluster/status") //nolint:noctx
		if err != nil {
			return ""
		}
		defer resp.Body.Close()
		var s map[string]any
		if decErr := json.NewDecoder(resp.Body).Decode(&s); decErr != nil {
			return ""
		}
		id, _ := s["leader_id"].(string)
		term, _ := s["term"].(float64)
		t.Logf("status check: leader_id=%q term=%.0f", id, term)
		return id
	}

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		for _, idx := range survivorIdxs {
			id := tryLeaderID(c.httpURLs[idx])
			if id != "" && id != leaderID {
				t.Logf("new leader elected: %s", id)
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	require.Failf(t, "no new leader elected",
		"two survivors must elect a new leader within 30s after killing %s", leaderID)
}
