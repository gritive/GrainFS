package server

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSSE_AuthBypass guards the dashboard's two SSE streams against
// regression of the bug PR #74 fixed: when SigV4 auth is wired, browser
// EventSource cannot send signed headers, so any stream that runs through
// the auth middleware returns 403 and the badge sticks at "reconnecting".
//
// /api/events and /api/events/heal/stream MUST stay reachable without
// credentials, same tier as /metrics and /api/cluster/status. The streams
// emit only public dashboard telemetry — no object data, no secrets.
func TestSSE_AuthBypass(t *testing.T) {
	base := setupAuthServer(t)

	for _, path := range []string{"/api/events", "/api/events/heal/stream"} {
		path := path
		t.Run(strings.TrimPrefix(path, "/api/"), func(t *testing.T) {
			client := &http.Client{Timeout: 2 * time.Second}
			req, _ := http.NewRequest(http.MethodGet, base+path, nil)
			resp, err := client.Do(req)
			require.NoError(t, err, "GET %s without auth must reach the handler", path)
			defer resp.Body.Close()

			assert.Equal(t, http.StatusOK, resp.StatusCode,
				"%s must return 200 without SigV4 — EventSource cannot sign requests", path)
			ct := resp.Header.Get("Content-Type")
			assert.True(t, strings.HasPrefix(ct, "text/event-stream"),
				"%s must keep SSE Content-Type even on auth-bypassed handler, got %q", path, ct)
		})
	}
}
