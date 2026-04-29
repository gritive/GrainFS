package e2e

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDashboardHealingCard_HTMLAndStream verifies that:
//
//  1. The dashboard HTML embeds the Phase 16 Self-Healing card markup so the
//     SRE sees the new section even before any HealEvents flow.
//  2. /api/events/heal/stream returns a long-lived SSE response with the
//     correct content-type and stays open for an EventSource client.
//
// Live HealEvent fan-out is exercised at the unit level in
// internal/server/heal_stream_test.go (TestHub_WriteSSE_HealCategoryOnly);
// reproducing that here would require triggering real EC corruption against
// running shards, which is out of scope for the Week 2 dashboard ship.
func TestDashboardHealingCard_HTMLAndStream(t *testing.T) {
	binary := getBinary()
	dir, err := os.MkdirTemp("", "grainfs-heal-card-e2e-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	port := freePort()
	cmd := exec.Command(binary, "serve",
		"--data", dir,
		"--port", fmt.Sprintf("%d", port),
		"--nfs4-port", "0",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())
	defer func() { _ = cmd.Process.Kill() }()

	waitForPort(t, port, 5*time.Second)
	endpoint := fmt.Sprintf("http://127.0.0.1:%d", port)

	t.Run("dashboard html contains self-healing card", func(t *testing.T) {
		resp, err := http.Get(endpoint + "/ui/")
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
	})

	t.Run("heal SSE endpoint responds with text/event-stream", func(t *testing.T) {
		// Use a short per-request deadline so the test never blocks on the
		// long-lived stream — we only need to verify the headers landed.
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/api/events/heal/stream", nil)
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
}
