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

// stubBalancer is a minimal BalancerInfo for testing.
type stubBalancer struct {
	result BalancerStatusResult
}

func (s *stubBalancer) Status() BalancerStatusResult { return s.result }

func TestBalancerStatus_NilBalancer_ReturnsUnavailable(t *testing.T) {
	// setupTestServer creates a server without a balancer → available: false
	base := setupTestServer(t)

	resp, err := http.Get(base + "/api/cluster/balancer/status")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, false, body["available"])
}

func TestBalancerStatus_WithBalancer_ReturnsStatus(t *testing.T) {
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })

	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	stub := &stubBalancer{
		result: BalancerStatusResult{
			Active:       true,
			ImbalancePct: 25.5,
			Nodes: []BalancerNodeInfo{
				{
					NodeID:         "node-a",
					DiskUsedPct:    70.0,
					DiskAvailBytes: 30 << 30,
					RequestsPerSec: 10.0,
					UpdatedAt:      time.Now(),
				},
				{
					NodeID:         "node-b",
					DiskUsedPct:    44.5,
					DiskAvailBytes: 55 << 30,
					RequestsPerSec: 5.0,
					UpdatedAt:      time.Now(),
				},
			},
		},
	}

	srv := New(addr, backend, WithBalancerInfo(stub))
	go srv.Run() //nolint:errcheck
	for i := 0; i < 50; i++ {
		conn, connErr := net.Dial("tcp", addr)
		if connErr == nil {
			conn.Close()
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	resp, err := http.Get("http://" + addr + "/api/cluster/balancer/status")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, true, body["available"])
	assert.Equal(t, true, body["active"])
	assert.InDelta(t, 25.5, body["imbalance_pct"], 0.01)

	nodes, ok := body["nodes"].([]any)
	require.True(t, ok)
	assert.Len(t, nodes, 2)
}
