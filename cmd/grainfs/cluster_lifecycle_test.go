package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/clusteradmin"
)

// startLifecycleStub spins up a UDS HTTP stub for lifecycle (transfer +
// drain) tests. The handler should multiplex the routes the test needs.
func startLifecycleStub(t *testing.T, h http.Handler) string {
	t.Helper()
	d, err := os.MkdirTemp("/tmp", "lc-")
	require.NoError(t, err)
	sock := filepath.Join(d, "admin.sock")
	ln, err := net.Listen("unix", sock)
	require.NoError(t, err)
	go http.Serve(ln, h) //nolint:errcheck
	t.Cleanup(func() { _ = ln.Close(); _ = os.RemoveAll(d) })
	return sock
}

func TestRunClusterTransferLeader_Happy(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/cluster/transfer-leader", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"old_leader": "n1", "term": 8})
	})
	sock := startLifecycleStub(t, mux)

	client := clusteradmin.NewClient(sock)
	var stdout bytes.Buffer
	err := runClusterTransferLeader(context.Background(), client, transferOpts{}, &stdout)
	require.NoError(t, err)
	require.Contains(t, stdout.String(), "old_leader")
	require.Contains(t, stdout.String(), "n1")
}

func TestRunClusterTransferLeader_NotLeader(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/cluster/transfer-leader", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": "not leader", "leader_id": "n2"})
	})
	sock := startLifecycleStub(t, mux)

	client := clusteradmin.NewClient(sock)
	var stdout bytes.Buffer
	err := runClusterTransferLeader(context.Background(), client, transferOpts{}, &stdout)
	require.Error(t, err)
	var tle *clusteradmin.TransferLeaderError
	require.True(t, errors.As(err, &tle))
	require.Equal(t, "n2", tle.LeaderID)
}

func TestRunClusterDrain_LeaderHappy(t *testing.T) {
	var transferCalls, removeCalls int
	leaderID := "n1"
	term := uint64(5)
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/cluster/status", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"mode":      "cluster",
			"node_id":   "self",
			"state":     "Leader",
			"leader_id": leaderID,
			"term":      term,
			"peers":     []string{"n1", "n2", "n3"},
		})
	})
	mux.HandleFunc("/v1/cluster/transfer-leader", func(w http.ResponseWriter, r *http.Request) {
		transferCalls++
		// After transfer, leader changes and term advances.
		leaderID = "n2"
		term = 6
		_ = json.NewEncoder(w).Encode(map[string]any{"old_leader": "n1", "term": 6})
	})
	mux.HandleFunc("/v1/cluster/remove-peer", func(w http.ResponseWriter, r *http.Request) {
		removeCalls++
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "removed", "id": "n1"})
	})
	sock := startLifecycleStub(t, mux)

	client := clusteradmin.NewClient(sock)
	var stdout bytes.Buffer
	err := runClusterDrain(context.Background(), client, "n1", drainOpts{Yes: true, Timeout: 5 * time.Second}, &stdout)
	require.NoError(t, err)
	require.Equal(t, 1, transferCalls)
	require.Equal(t, 1, removeCalls)
	out := stdout.String()
	require.Contains(t, out, "leadership transferred to n2")
	require.Contains(t, out, "drained")
	require.Contains(t, out, "rejoin with")
}

func TestRunClusterDrain_FollowerHappy(t *testing.T) {
	var transferCalls, removeCalls int
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/cluster/status", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"mode":      "cluster",
			"leader_id": "n1",
			"term":      5,
			"peers":     []string{"n1", "n2", "n3"},
		})
	})
	mux.HandleFunc("/v1/cluster/transfer-leader", func(w http.ResponseWriter, r *http.Request) {
		transferCalls++
	})
	mux.HandleFunc("/v1/cluster/remove-peer", func(w http.ResponseWriter, r *http.Request) {
		removeCalls++
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "removed", "id": "n2"})
	})
	sock := startLifecycleStub(t, mux)

	client := clusteradmin.NewClient(sock)
	var stdout bytes.Buffer
	err := runClusterDrain(context.Background(), client, "n2", drainOpts{Yes: true, Timeout: 5 * time.Second}, &stdout)
	require.NoError(t, err)
	require.Equal(t, 0, transferCalls, "follower drain must not call transfer")
	require.Equal(t, 1, removeCalls)
}

func TestRunClusterDrain_TransferNoPeers(t *testing.T) {
	var removeCalls int
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/cluster/status", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"mode":      "cluster",
			"leader_id": "n1",
			"term":      5,
			"peers":     []string{"n1"},
		})
	})
	mux.HandleFunc("/v1/cluster/transfer-leader", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": "single-node mode"})
	})
	mux.HandleFunc("/v1/cluster/remove-peer", func(w http.ResponseWriter, r *http.Request) {
		removeCalls++
	})
	sock := startLifecycleStub(t, mux)

	client := clusteradmin.NewClient(sock)
	var stdout bytes.Buffer
	err := runClusterDrain(context.Background(), client, "n1", drainOpts{Yes: true, Timeout: 5 * time.Second}, &stdout)
	require.Error(t, err)
	require.Contains(t, err.Error(), "transfer")
	require.Equal(t, 0, removeCalls, "transfer fail → no remove (A2-a, voter set unchanged)")
}

func TestRunClusterDrain_TransferOkRemoveFail(t *testing.T) {
	leaderID := "n1"
	term := uint64(5)
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/cluster/status", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"mode":      "cluster",
			"leader_id": leaderID,
			"term":      term,
			"peers":     []string{"n1", "n2", "n3"},
		})
	})
	mux.HandleFunc("/v1/cluster/transfer-leader", func(w http.ResponseWriter, r *http.Request) {
		leaderID = "n2"
		term = 6
		_ = json.NewEncoder(w).Encode(map[string]any{"old_leader": "n1", "term": 6})
	})
	mux.HandleFunc("/v1/cluster/remove-peer", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": "quorum would break"})
	})
	sock := startLifecycleStub(t, mux)

	client := clusteradmin.NewClient(sock)
	var stdout bytes.Buffer
	err := runClusterDrain(context.Background(), client, "n1", drainOpts{Yes: true, Timeout: 5 * time.Second}, &stdout)
	require.Error(t, err)
	out := stdout.String()
	require.Contains(t, out, "leadership transferred")
	require.Contains(t, out, "voter removal failed")
	require.Contains(t, out, "still in voter set")
}

func TestRunClusterDrain_LeaderRequiresYes(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/cluster/status", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"mode":      "cluster",
			"leader_id": "n1",
			"term":      5,
			"peers":     []string{"n1", "n2", "n3"},
		})
	})
	sock := startLifecycleStub(t, mux)

	client := clusteradmin.NewClient(sock)
	var stdout bytes.Buffer
	err := runClusterDrain(context.Background(), client, "n1", drainOpts{Yes: false}, &stdout)
	require.Error(t, err)
	require.Contains(t, err.Error(), "--yes")
}
