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

// TestSplitBrain_MetricExposed verifies that the Prometheus /metrics endpoint
// exposes the grainfs_split_brain_suspected metric (always 0 in solo mode).
func TestSplitBrain_MetricExposed(t *testing.T) {
	resp, err := http.Get(testServerURL + "/metrics")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	assert.Contains(t, string(body), "grainfs_split_brain_suspected",
		"/metrics must expose split brain indicator")
}

// TestSplitBrain_SoloIsZero verifies that in solo mode split brain is never suspected.
func TestSplitBrain_SoloIsZero(t *testing.T) {
	resp, err := http.Get(testServerURL + "/metrics")
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	for _, line := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(line, "grainfs_split_brain_suspected") && !strings.HasPrefix(line, "#") {
			assert.Contains(t, line, "0", "solo mode must report split_brain_suspected=0, got: %q", line)
			return
		}
	}
	t.Fatal("grainfs_split_brain_suspected metric not found in /metrics output")
}

// TestSplitBrain_ClusterStatusField verifies /api/cluster/status includes split_brain_suspected.
func TestSplitBrain_ClusterStatusField(t *testing.T) {
	resp, err := http.Get(testServerURL + "/api/cluster/status")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var status map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&status))

	_, ok := status["split_brain_suspected"]
	assert.True(t, ok, "/api/cluster/status must include split_brain_suspected field")

	assert.Equal(t, false, status["split_brain_suspected"],
		"solo mode must report split_brain_suspected=false")
}
