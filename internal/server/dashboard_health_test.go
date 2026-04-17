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

// TestBadgerHealth_Localhost verifies GET /admin/health/badger returns 200+JSON from localhost.
func TestBadgerHealth_Localhost(t *testing.T) {
	base := setupTestServer(t)

	resp, err := http.Get(base + "/admin/health/badger")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Contains(t, body, "available")
	// Test server uses LocalBackend → no BadgerDB → available: false
	assert.Equal(t, false, body["available"])
}

// TestBadgerHealth_RemoteDenied verifies that non-localhost is rejected with 403.
func TestBadgerHealth_RemoteDenied(t *testing.T) {
	extIP := getNonLoopbackIPv4()
	if extIP == "" {
		t.Skip("no non-loopback IPv4 interface found")
	}

	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })

	port := freePort(t)
	srv := New(fmt.Sprintf("0.0.0.0:%d", port), backend)
	go srv.Run() //nolint:errcheck

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
	require.True(t, started)

	resp, err := http.Get(fmt.Sprintf("http://%s:%d/admin/health/badger", extIP, port))
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
}

// TestRaftHealth_Localhost verifies GET /admin/health/raft returns 200+JSON from localhost.
func TestRaftHealth_Localhost(t *testing.T) {
	base := setupTestServer(t)

	resp, err := http.Get(base + "/admin/health/raft")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Contains(t, body, "available")
	// Test server has no cluster → available: false
	assert.Equal(t, false, body["available"])
}

// TestRaftHealth_RemoteDenied verifies that non-localhost is rejected with 403.
func TestRaftHealth_RemoteDenied(t *testing.T) {
	extIP := getNonLoopbackIPv4()
	if extIP == "" {
		t.Skip("no non-loopback IPv4 interface found")
	}

	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })

	port := freePort(t)
	srv := New(fmt.Sprintf("0.0.0.0:%d", port), backend)
	go srv.Run() //nolint:errcheck

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
	require.True(t, started)

	resp, err := http.Get(fmt.Sprintf("http://%s:%d/admin/health/raft", extIP, port))
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
}

// TestBucketsEC_Localhost verifies GET /admin/buckets/ec returns 200+JSON from localhost.
func TestBucketsEC_Localhost(t *testing.T) {
	base := setupTestServer(t)

	resp, err := http.Get(base + "/admin/buckets/ec")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Contains(t, body, "buckets")
}

// TestBucketsEC_RemoteDenied verifies that non-localhost is rejected with 403.
func TestBucketsEC_RemoteDenied(t *testing.T) {
	extIP := getNonLoopbackIPv4()
	if extIP == "" {
		t.Skip("no non-loopback IPv4 interface found")
	}

	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })

	port := freePort(t)
	srv := New(fmt.Sprintf("0.0.0.0:%d", port), backend)
	go srv.Run() //nolint:errcheck

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
	require.True(t, started)

	resp, err := http.Get(fmt.Sprintf("http://%s:%d/admin/buckets/ec", extIP, port))
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
}
