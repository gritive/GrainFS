package main

import (
	"bytes"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
