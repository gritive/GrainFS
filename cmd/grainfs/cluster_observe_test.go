package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Thin invocation tests for peers/events cobra wrappers. Rendering, filtering,
// and orchestration are tested in internal/clusteradmin.

func TestCmdPeers_FlagsReachPackage(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/cluster/status", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"mode":  "cluster",
			"peers": []string{"n2"},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	cmd := clusterPeersCmd()
	out := &bytes.Buffer{}
	cmd.SetOut(out)
	cmd.SetErr(out)
	cmd.SetArgs([]string{"--endpoint", srv.URL})
	require.NoError(t, cmd.Execute())
	assert.NotEmpty(t, out.String(), "cobra wrapper must emit the package's rendered output")
}

func TestCmdEvents_FlagsReachPackage(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/eventlog", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"ts": 1, "type": "system", "action": "cluster-remove-peer"},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	cmd := clusterEventsCmd()
	out := &bytes.Buffer{}
	cmd.SetOut(out)
	cmd.SetErr(out)
	cmd.SetArgs([]string{"--endpoint", srv.URL})
	require.NoError(t, cmd.Execute())
	assert.Contains(t, out.String(), "cluster-remove-peer", "package's rendered output must reach stdout")
}
