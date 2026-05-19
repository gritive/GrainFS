package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// dashboardFactory mirrors volumeScrubFactory. TokenURLAndRotate rotates the
// dashboard token; isolating each case in its own fixture prevents one
// rotate from invalidating another case's expectations.
type dashboardFactory func(args ...string) s3Target

// TestDashboardE2E exercises the Web UI surface (static serve, Phase 16
// Self-Healing card markup + SSE, dashboard CLI token rotate) against both
// single-node and 4-node cluster fixtures. The cluster branch is the first
// end-to-end coverage of these endpoints on a 4-node DynamicJoin fixture
// and may surface placement / leader-redirect quirks — captured as signals,
// not fixed in this PR.
func TestDashboardE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runDashboardCases(t, func(args ...string) s3Target {
			return newDedicatedSingleNodeS3Target(t, args)
		})
	})
	t.Run("Cluster4Node", func(t *testing.T) {
		runDashboardCases(t, func(args ...string) s3Target {
			return newClusterS3TargetWithExtraArgs(t, 4, args)
		})
	})
}

func runDashboardCases(t *testing.T, mk dashboardFactory) {
	t.Helper()

	t.Run("Serves", func(t *testing.T) {
		tgt := mk()
		resp, err := http.Get(tgt.endpoint(0) + "/ui/")
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)
		body, _ := io.ReadAll(resp.Body)
		s := string(body)
		assert.Contains(t, s, "GrainFS")
		assert.Contains(t, s, "<!DOCTYPE html>")
	})

	t.Run("HealingCardHTMLMarkup", func(t *testing.T) {
		tgt := mk()
		resp, err := http.Get(tgt.endpoint(0) + "/ui/")
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		html := string(body)

		assert.Contains(t, html, `id="heal-section"`, "Self-Healing card section missing")
		assert.Contains(t, html, `id="heal-last-when"`, "Last Heal value element missing")
		assert.Contains(t, html, `id="heal-rate"`, "Heal Rate element missing")
		assert.Contains(t, html, `id="heal-restart-count"`, "Restart Recovery line missing")
		assert.Contains(t, html, `id="heal-events-table"`, "Live Heal Events table missing")
		assert.Contains(t, html, `/api/events/heal/stream`, "heal SSE endpoint not wired in JS")
		assert.Contains(t, html, `id="incident-section"`, "Zero-ops incident section missing")
		assert.Contains(t, html, `id="incident-table"`, "Incident table missing")
		assert.Contains(t, html, `/api/incidents`, "incident API not wired in JS")
		assert.Contains(t, html, `FD exhaustion risk`, "FD incident label missing")
	})

	t.Run("HealingCardSSEStream", func(t *testing.T) {
		tgt := mk()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, tgt.endpoint(0)+"/api/events/heal/stream", nil)
		require.NoError(t, err)

		client := &http.Client{Timeout: 3 * time.Second}
		resp, err := client.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode)
		ct := resp.Header.Get("Content-Type")
		assert.True(t, strings.HasPrefix(ct, "text/event-stream"),
			"expected SSE content-type, got %q", ct)
		assert.Equal(t, "no-cache", resp.Header.Get("Cache-Control"))
	})

	t.Run("TokenURLAndRotate", func(t *testing.T) {
		tgt := mk()
		dataDir := dashboardDataDir(tgt)
		port := dashboardPort(tgt, 0)

		out1, code := runCLI(t, dataDir, "dashboard", "--format", "json")
		require.Equal(t, 0, code, out1)
		var resp1 struct {
			Token string `json:"token"`
			URL   string `json:"url"`
		}
		require.NoError(t, json.Unmarshal([]byte(out1), &resp1))
		require.NotEmpty(t, resp1.Token)
		require.Contains(t, resp1.URL, "#token="+resp1.Token)

		require.Equal(t, http.StatusOK, callUI(t, port, resp1.Token), "old token must work")
		require.Equal(t, http.StatusUnauthorized, callUI(t, port, ""), "no token must 401")

		out2, code := runCLI(t, dataDir, "dashboard", "--rotate", "--format", "json")
		require.Equal(t, 0, code, out2)
		var resp2 struct {
			Token string `json:"token"`
		}
		require.NoError(t, json.Unmarshal([]byte(out2), &resp2))
		require.NotEqual(t, resp1.Token, resp2.Token)

		require.Equal(t, http.StatusUnauthorized, callUI(t, port, resp1.Token), "rotated old token must be dead")
		require.Equal(t, http.StatusOK, callUI(t, port, resp2.Token), "new token must work")
	})
}

// dashboardDataDir returns the dataDir to drive the admin CLI against tgt
// (single → unique server dir; cluster → leader).
func dashboardDataDir(tgt s3Target) string {
	return strings.TrimSuffix(tgt.adminSockPath(), "/admin.sock")
}

// dashboardPort parses the HTTP port out of tgt.endpoint(nodeIdx).
func dashboardPort(tgt s3Target, nodeIdx int) int {
	ep := tgt.endpoint(nodeIdx)
	i := strings.LastIndex(ep, ":")
	if i < 0 {
		panic(fmt.Sprintf("endpoint has no port: %s", ep))
	}
	var port int
	if _, err := fmt.Sscanf(ep[i+1:], "%d", &port); err != nil {
		panic(fmt.Sprintf("parse port from %s: %v", ep, err))
	}
	return port
}

func callUI(t *testing.T, port int, token string) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/ui/api/volumes", port), nil)
	require.NoError(t, err)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}
