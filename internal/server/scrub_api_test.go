package server

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// TestScrubAPI_Localhost verifies that GET /admin/health/scrub is accessible
// from localhost and returns valid JSON (200 OK). The localhostOnly() middleware
// must pass through connections from 127.0.0.1.
func TestScrubAPI_Localhost(t *testing.T) {
	base := setupTestServer(t)

	resp, err := http.Get(base + "/admin/health/scrub")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var body scrubStatsResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	// No scrubber attached to the test server → available: false, but endpoint is reachable.
	assert.False(t, body.Available, "test server has no scrubber; available must be false")
}

// TestScrubAPI_RemoteDenied verifies that a connection from a non-localhost IP
// is rejected with 403 Forbidden. This test requires a non-loopback interface;
// it is skipped when only loopback is available (e.g. some CI containers).
func TestScrubAPI_RemoteDenied(t *testing.T) {
	extIP := getNonLoopbackIPv4()
	if extIP == "" {
		t.Skip("no non-loopback IPv4 interface found; skipping external access test")
	}

	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })

	// Bind to all interfaces so we can reach the server via the non-loopback IP.
	port := freePort(t)
	addr := fmt.Sprintf("0.0.0.0:%d", port)
	srv := New(addr, backend)
	go srv.Run() //nolint:errcheck

	// Wait for the server to start.
	started := false
	for i := 0; i < 50; i++ {
		conn, dialErr := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 100*time.Millisecond)
		if dialErr == nil {
			conn.Close()
			started = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	require.True(t, started, "server did not start in time")

	// Connect via the non-loopback IP → server sees remote addr = extIP → not localhost → 403.
	url := fmt.Sprintf("http://%s:%d/admin/health/scrub", extIP, port)
	resp, err := http.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusForbidden, resp.StatusCode,
		"admin endpoint must return 403 for connections from non-localhost IP %s", extIP)
}

// getNonLoopbackIPv4 returns the first non-loopback IPv4 address on this host,
// or empty string if none is found.
func getNonLoopbackIPv4() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, a := range addrs {
		ipnet, ok := a.(*net.IPNet)
		if !ok {
			continue
		}
		ip := ipnet.IP
		if !ip.IsLoopback() && ip.To4() != nil {
			return ip.String()
		}
	}
	return ""
}
