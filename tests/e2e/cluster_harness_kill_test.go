package e2e

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// nodeSettled returns true when the node at baseURL reports peers==2 (3-node cluster).
// Safe to call inside require.Eventually — does not use require internally.
func nodeSettled(baseURL string) bool {
	resp, err := http.Get(baseURL + "/api/cluster/status")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	out := map[string]any{}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return false
	}
	return len(stringList(out["peers"])) == 2
}

func TestE2EClusterKillAndRestart(t *testing.T) {
	t.Run("Cluster3Node", func(t *testing.T) {
		c := startE2ECluster(t, e2eClusterOptions{
			Nodes:      3,
			Mode:       ClusterModeDynamicJoin,
			ClusterKey: "E2E-HARNESS-KILL",
			LogPrefix:  "harness-kill",
			DisableNFS: true, DisableNBD: true,
		})

		victim := (c.leaderIdx + 1) % 3
		c.KillNode(victim)
		waitClusterSettled(t, c.httpURLs[c.leaderIdx])

		c.RestartNode(t, victim)
		require.Eventually(t, func() bool {
			return nodeSettled(c.httpURLs[victim])
		}, 90*time.Second, 500*time.Millisecond, "restarted node never settled")
	})
}
