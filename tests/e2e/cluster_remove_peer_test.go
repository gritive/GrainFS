package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestE2E_ClusterRemovePeer_DeadFollower spins up a 3-node cluster, kills a
// follower, and verifies the operator can evict it via the new
// /api/cluster/remove-peer endpoint. Validates the full chain: pre-flight
// allows it (alive_after >= new_quorum), joint consensus commits, status
// reflects the shrunk voter set, audit event surfaces.
func TestE2E_ClusterRemovePeer_DeadFollower(t *testing.T) {

	c := startE2ECluster(t, e2eClusterOptions{
		Nodes:      3,
		Mode:       ClusterModeDynamicJoin,
		ClusterKey: "E2E-REMOVE-PEER-KEY",
		AccessKey:  "rm-ak",
		SecretKey:  "rm-sk",
		LogPrefix:  "grainfs-remove-peer",
	})

	leaderIdx := c.leaderIdx
	require.GreaterOrEqual(t, leaderIdx, 0, "harness must have identified leader")

	// Pick the first non-leader.
	followerIdx := -1
	for i := range c.procs {
		if i != leaderIdx {
			followerIdx = i
			break
		}
	}
	require.GreaterOrEqual(t, followerIdx, 0)
	// /api/cluster/status.peers reports node IDs, and remove-peer accepts the
	// same identifier.
	deadID := c.nodeID(followerIdx)
	leaderURL := c.httpURLs[leaderIdx]

	// Wait for the dynamic-join membership to settle. Leader's Peers() excludes
	// self, so a 3-node cluster should report 2 remote voters once joins commit.
	var lastStatus map[string]any
	settled := false
	for i := 0; i < 120; i++ {
		s := getStatusJSON(t, leaderURL)
		lastStatus = s
		voters := stringList(s["peers"])
		if len(voters) == 2 && containsString(voters, deadID) {
			settled = true
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !settled {
		t.Fatalf("leader never observed 2 remote voters incl %s; last status: %+v", deadID, lastStatus)
	}

	// Kill the follower hard so it never rejoins during the test.
	require.NoError(t, c.procs[followerIdx].Process.Signal(syscall.SIGKILL))
	_ = c.procs[followerIdx].Wait()
	c.procs[followerIdx] = nil

	// Happy path with no --force: LivePeers reports all metaRaft voters as
	// alive (PR-D will refine this with real liveness). Pre-flight math —
	// 3 voters → 2, alive_after=2, new_quorum=2 — passes; engine commits.
	body, _ := json.Marshal(map[string]any{"id": deadID, "force": false})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, leaderURL+"/api/cluster/remove-peer", bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	respBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "remove-peer must succeed, body=%s", string(respBody))

	// After remove, leader's remote voter set shrinks from 2 to 1, no longer contains the removed id.
	require.Eventually(t, func() bool {
		s := getStatusJSON(t, leaderURL)
		voters := stringList(s["peers"])
		return len(voters) == 1 && !containsString(voters, deadID)
	}, 30*time.Second, 500*time.Millisecond, "voter set must shrink to 1 remote without the removed id")

	// Audit event must surface in the event log.
	events := getEventLog(t, leaderURL)
	found := false
	for _, e := range events {
		if action, _ := e["action"].(string); action == "cluster-remove-peer" {
			found = true
			break
		}
	}
	assert.True(t, found, "cluster-remove-peer event must appear in /api/eventlog")
}

func getStatusJSON(t *testing.T, base string) map[string]any {
	t.Helper()
	resp, err := http.Get(base + "/api/cluster/status")
	require.NoError(t, err)
	defer resp.Body.Close()
	out := map[string]any{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	return out
}

func getEventLog(t *testing.T, base string) []map[string]any {
	t.Helper()
	resp, err := http.Get(base + "/api/eventlog?since=300&limit=200")
	require.NoError(t, err)
	defer resp.Body.Close()
	var events []map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&events))
	return events
}

func stringList(v any) []string {
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(arr))
	for _, x := range arr {
		if s, ok := x.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func containsString(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}
