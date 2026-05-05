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

// Thin invocation tests for the peers/events cobra wrappers. Rendering and
// filtering rules live in internal/clusteradmin and are tested there.

func TestCmdPeers_HitsStatusAndRenders(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/cluster/status", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"mode":       "cluster",
			"leader_id":  "n1",
			"peers":      []string{"n1", "n2", "n3"},
			"down_nodes": []string{"n3"},
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
	assert.Contains(t, out.String(), "leader")
	assert.Contains(t, out.String(), "down")
}

func TestCmdEvents_HitsEventLogAndRenders(t *testing.T) {
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
	assert.Contains(t, out.String(), "cluster-remove-peer")
}
