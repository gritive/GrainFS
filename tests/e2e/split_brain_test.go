package e2e

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSplitBrainE2E asserts the grainfs_split_brain_suspected metric is
// exposed and reads zero on the shared fixtures (no split observed). Shared
// single + shared cluster — Prometheus scrape is read-only, no state
// mutation across sub-tests.
func TestSplitBrainE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runSplitBrainCases(t, newSingleNodeS3Target())
	})
	t.Run("Cluster4Node", func(t *testing.T) {
		runSplitBrainCases(t, newSharedClusterS3Target(t))
	})
}

func runSplitBrainCases(t *testing.T, tgt s3Target) {
	t.Helper()
	endpoint := tgt.endpoint(0)

	t.Run("MetricExposed", func(t *testing.T) {
		resp, err := http.Get(endpoint + "/metrics")
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Contains(t, string(body), "grainfs_split_brain_suspected",
			"/metrics must expose split brain indicator")
	})

	t.Run("ValueIsZero", func(t *testing.T) {
		resp, err := http.Get(endpoint + "/metrics")
		require.NoError(t, err)
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		for _, line := range strings.Split(string(body), "\n") {
			if strings.HasPrefix(line, "grainfs_split_brain_suspected") && !strings.HasPrefix(line, "#") {
				assert.Contains(t, line, "0",
					"split_brain_suspected must be 0 in steady state, got: %q", line)
				return
			}
		}
		t.Fatal("grainfs_split_brain_suspected metric not found in /metrics output")
	})
}
