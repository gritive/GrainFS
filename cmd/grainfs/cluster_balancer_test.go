package main

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/clusteradmin"
)

func startBalancerStub(t *testing.T, h http.Handler) string {
	t.Helper()
	d, err := os.MkdirTemp("/tmp", "bal-")
	require.NoError(t, err)
	sock := filepath.Join(d, "admin.sock")
	ln, err := net.Listen("unix", sock)
	require.NoError(t, err)
	go http.Serve(ln, h) //nolint:errcheck
	t.Cleanup(func() { _ = ln.Close(); _ = os.RemoveAll(d) })
	return sock
}

func TestRunClusterBalancerStatus_Active(t *testing.T) {
	payload := `{
		"available":true,"active":true,"imbalance_pct":3.4,
		"nodes":[
			{"node_id":"n1","disk_used_pct":84.0,"disk_avail_bytes":107374182400,"requests_per_sec":42},
			{"node_id":"n2","disk_used_pct":80.0,"disk_avail_bytes":130000000000,"requests_per_sec":40}
		]
	}`
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/cluster/balancer/status", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(payload))
	})
	sock := startBalancerStub(t, mux)

	client := clusteradmin.NewClient(sock)
	var stdout bytes.Buffer
	require.NoError(t, runClusterBalancerStatus(context.Background(), client, "text", &stdout))
	out := stdout.String()
	assert.Contains(t, out, "balancer:")
	assert.Contains(t, out, "active")
	assert.Contains(t, out, "imbalance:")
	assert.Contains(t, out, "NODES")
	assert.Contains(t, out, "n1")
}

func TestRunClusterBalancerStatus_Disabled(t *testing.T) {
	payload := `{"available":true,"active":false,"nodes":[]}`
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/cluster/balancer/status", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(payload))
	})
	sock := startBalancerStub(t, mux)

	client := clusteradmin.NewClient(sock)
	var stdout bytes.Buffer
	require.NoError(t, runClusterBalancerStatus(context.Background(), client, "text", &stdout))
	assert.Contains(t, stdout.String(), "balancer: disabled")
}

func TestRunClusterBalancerStatus_NotAvailable(t *testing.T) {
	payload := `{"available":false}`
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/cluster/balancer/status", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(payload))
	})
	sock := startBalancerStub(t, mux)

	client := clusteradmin.NewClient(sock)
	var stdout bytes.Buffer
	require.NoError(t, runClusterBalancerStatus(context.Background(), client, "text", &stdout))
	assert.Contains(t, stdout.String(), "not available")
}
