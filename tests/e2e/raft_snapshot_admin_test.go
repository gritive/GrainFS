package e2e

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRaftSnapshotAdminE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runRaftSnapshotAdminCases(t, newSingleNodeS3Target())
	})
	t.Run("Cluster4Node", func(t *testing.T) {
		runRaftSnapshotAdminCases(t, newSharedClusterS3Target(t))
	})
}

func runRaftSnapshotAdminCases(t *testing.T, tgt s3Target) {
	t.Helper()
	endpoint := tgt.endpoint(0)

	t.Run("TriggerStatusAndMetrics", func(t *testing.T) {
		_ = tgt.uniqueBucket(t, "raftsnap")

		resp, err := http.Post(endpoint+"/admin/raft/snapshot", "application/json", nil) //nolint:noctx
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)

		var trigger struct {
			Index     uint64 `json:"index"`
			Term      uint64 `json:"term"`
			SizeBytes int    `json:"size_bytes"`
		}
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&trigger))
		require.NotZero(t, trigger.Index)
		require.NotZero(t, trigger.Term)
		require.Positive(t, trigger.SizeBytes)

		statusResp, err := http.Get(endpoint + "/admin/raft/snapshot") //nolint:noctx
		require.NoError(t, err)
		defer statusResp.Body.Close()
		require.Equal(t, http.StatusOK, statusResp.StatusCode)

		var status struct {
			Available bool   `json:"available"`
			Index     uint64 `json:"index"`
			Term      uint64 `json:"term"`
			SizeBytes int    `json:"size_bytes"`
		}
		require.NoError(t, json.NewDecoder(statusResp.Body).Decode(&status))
		assert.True(t, status.Available)
		assert.Equal(t, trigger.Index, status.Index)
		assert.Equal(t, trigger.Term, status.Term)
		assert.Equal(t, trigger.SizeBytes, status.SizeBytes)

		metricsResp, err := http.Get(endpoint + "/metrics") //nolint:noctx
		require.NoError(t, err)
		defer metricsResp.Body.Close()
		require.Equal(t, http.StatusOK, metricsResp.StatusCode)
		body, err := io.ReadAll(metricsResp.Body)
		require.NoError(t, err)
		metrics := string(body)
		assert.Contains(t, metrics, "grainfs_raft_snapshot_trigger_total")
		assert.Contains(t, metrics, "grainfs_raft_snapshot_last_index")
		assert.True(t, strings.Contains(metrics, "grainfs_raft_snapshot_last_size_bytes"))
	})
}
