package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/clusteradmin"
)

// Thin invocation tests for peers/events cobra wrappers. Rendering, filtering,
// and orchestration are tested in internal/clusteradmin.

// startObserveUDSStub brings up a stub HTTP handler bound to a Unix socket.
// Mirrors startUDSStubServer in cluster_remove_peer_test.go.
func startObserveUDSStub(t *testing.T, h http.Handler) string {
	t.Helper()
	d, err := os.MkdirTemp("/tmp", "ob-")
	require.NoError(t, err)
	sock := filepath.Join(d, "admin.sock")
	ln, err := net.Listen("unix", sock)
	require.NoError(t, err)
	go http.Serve(ln, h) //nolint:errcheck
	t.Cleanup(func() {
		_ = ln.Close()
		_ = os.RemoveAll(d)
	})
	return sock
}

func TestCmdPeers_FlagsReachPackage(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/cluster/status", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"mode":  "cluster",
			"peers": []string{"n2"},
		})
	})
	sock := startObserveUDSStub(t, mux)

	cmd := clusterPeersCmd()
	cmd.Flags().String("endpoint", "", "")
	cmd.Flags().String("format", "text", "")
	out := &bytes.Buffer{}
	cmd.SetOut(out)
	cmd.SetErr(out)
	cmd.SetArgs([]string{"--endpoint", sock})
	require.NoError(t, cmd.Execute())
	assert.NotEmpty(t, out.String(), "cobra wrapper must emit the package's rendered output")
}

func TestCmdEvents_FlagsReachPackage(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/cluster/eventlog", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"ts": 1, "type": "system", "action": "cluster-remove-peer"},
		})
	})
	sock := startObserveUDSStub(t, mux)

	cmd := clusterEventsCmd()
	cmd.Flags().String("endpoint", "", "")
	cmd.Flags().String("format", "text", "")
	out := &bytes.Buffer{}
	cmd.SetOut(out)
	cmd.SetErr(out)
	cmd.SetArgs([]string{"--endpoint", sock})
	require.NoError(t, cmd.Execute())
	assert.Contains(t, out.String(), "cluster-remove-peer", "package's rendered output must reach stdout")
}

func TestRunClusterHealth_TextHealthy(t *testing.T) {
	payload := `{
		"mode":"cluster","degraded":false,"leader_id":"n1","term":7,
		"quorum":{"voters_total":3,"alive_count":3,"required":2,"healthy":true},
		"peers":[{"peer_id":"n1","state":"self"},{"peer_id":"n2","state":"live"},{"peer_id":"n3","state":"live"}],
		"issues":[]
	}`
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/cluster/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(payload))
	})
	sock := startObserveUDSStub(t, mux)

	client := clusteradmin.NewClient(sock)
	var stdout bytes.Buffer
	require.NoError(t, runClusterHealth(context.Background(), client, "text", &stdout))
	out := stdout.String()
	assert.Contains(t, out, "mode:")
	assert.Contains(t, out, "cluster")
	assert.Contains(t, out, "quorum:")
	assert.Contains(t, out, "3/3")
	assert.Contains(t, out, "PEERS")
	assert.Contains(t, out, "(none)")
}

func TestRunClusterHealth_TextWithIssues(t *testing.T) {
	payload := `{
		"mode":"cluster","degraded":true,
		"quorum":{"voters_total":3,"alive_count":2,"required":2,"healthy":true},
		"peers":[{"peer_id":"n1","state":"self"},{"peer_id":"n2","state":"live"},{"peer_id":"n3","state":"down"}],
		"issues":["voter n3 down — investigate","EC degraded mode"]
	}`
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/cluster/health", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(payload))
	})
	sock := startObserveUDSStub(t, mux)

	client := clusteradmin.NewClient(sock)
	var stdout bytes.Buffer
	require.NoError(t, runClusterHealth(context.Background(), client, "text", &stdout))
	out := stdout.String()
	assert.Contains(t, out, "voter n3 down")
	assert.Contains(t, out, "EC degraded")
}

func TestRunClusterPlacement_Full(t *testing.T) {
	payload := `{
		"mode":"cluster",
		"shard_groups":[
			{"id":"group-0","peer_ids":["n1","n2","n3"]},
			{"id":"group-1","peer_ids":["n2","n3","n4"]}
		],
		"bucket_assignments":{"default":"group-0","bench":"group-1"}
	}`
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/cluster/status", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(payload))
	})
	sock := startObserveUDSStub(t, mux)

	client := clusteradmin.NewClient(sock)
	var stdout bytes.Buffer
	require.NoError(t, runClusterPlacement(context.Background(), client, "", "text", &stdout))
	out := stdout.String()
	assert.Contains(t, out, "SHARD GROUPS")
	assert.Contains(t, out, "group-0")
	assert.Contains(t, out, "group-1")
	assert.Contains(t, out, "BUCKET ASSIGNMENTS")
	assert.Contains(t, out, "default")
	assert.Contains(t, out, "bench")
}

func TestRunClusterPlacement_SpecificBucket(t *testing.T) {
	payload := `{
		"mode":"cluster",
		"shard_groups":[{"id":"group-1","peer_ids":["n2","n3","n4"]}],
		"bucket_assignments":{"bench":"group-1"}
	}`
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/cluster/status", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(payload))
	})
	sock := startObserveUDSStub(t, mux)

	client := clusteradmin.NewClient(sock)
	var stdout bytes.Buffer
	require.NoError(t, runClusterPlacement(context.Background(), client, "bench", "text", &stdout))
	out := stdout.String()
	assert.Contains(t, out, "bench")
	assert.Contains(t, out, "group-1")
	assert.Contains(t, out, "n2, n3, n4")
	assert.NotContains(t, out, "SHARD GROUPS", "specific bucket query suppresses full table")
}

func TestRunClusterPlacement_UnknownBucket(t *testing.T) {
	payload := `{"mode":"cluster","bucket_assignments":{"default":"group-0"}}`
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/cluster/status", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(payload))
	})
	sock := startObserveUDSStub(t, mux)

	client := clusteradmin.NewClient(sock)
	var stdout bytes.Buffer
	require.NoError(t, runClusterPlacement(context.Background(), client, "missing", "text", &stdout))
	assert.Contains(t, stdout.String(), "not assigned")
}

func TestRunClusterPlacement_LocalMode(t *testing.T) {
	payload := `{"mode":"local"}`
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/cluster/status", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(payload))
	})
	sock := startObserveUDSStub(t, mux)

	client := clusteradmin.NewClient(sock)
	var stdout bytes.Buffer
	require.NoError(t, runClusterPlacement(context.Background(), client, "", "text", &stdout))
	assert.Contains(t, stdout.String(), "single-node mode")
}
