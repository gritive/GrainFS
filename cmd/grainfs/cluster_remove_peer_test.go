package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Tests here exercise the cobra command wiring (flag parsing, prompt
// behaviour, error surfacing). HTTP-shape and pre-flight rules are covered
// in internal/clusteradmin tests so this layer stays thin.

func runRemovePeer(t *testing.T, base string, args []string, stdin string) (string, error) {
	t.Helper()
	cmd := clusterRemovePeerCmd()
	out := &bytes.Buffer{}
	cmd.SetOut(out)
	cmd.SetErr(out)
	cmd.SetIn(strings.NewReader(stdin))
	cmd.SetArgs(append([]string{"--endpoint", base}, args...))
	err := cmd.Execute()
	return out.String(), err
}

type stubServer struct {
	statusBody  map[string]any
	removeCode  int
	removeBody  map[string]any
	removeCalls atomic.Int32
	removedBody atomic.Value // map[string]any
}

func (s *stubServer) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/cluster/status", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(s.statusBody)
	})
	mux.HandleFunc("/api/cluster/remove-peer", func(w http.ResponseWriter, r *http.Request) {
		s.removeCalls.Add(1)
		body, _ := io.ReadAll(r.Body)
		parsed := map[string]any{}
		_ = json.Unmarshal(body, &parsed)
		s.removedBody.Store(parsed)
		code := s.removeCode
		if code == 0 {
			code = http.StatusOK
		}
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(s.removeBody)
	})
	return mux
}

func TestCmdRemovePeer_Yes_PostsAndPrintsConfirmation(t *testing.T) {
	stub := &stubServer{
		statusBody: map[string]any{
			"mode":      "cluster",
			"state":     "Leader",
			"leader_id": "n1",
			"peers":     []string{"n1", "n2", "n3"},
		},
		removeBody: map[string]any{"status": "removed", "id": "n3"},
	}
	srv := httptest.NewServer(stub.handler())
	defer srv.Close()

	out, err := runRemovePeer(t, srv.URL, []string{"n3", "--yes"}, "")
	require.NoError(t, err, out)
	assert.Contains(t, out, "removed n3")
	assert.Equal(t, int32(1), stub.removeCalls.Load())
}

func TestCmdRemovePeer_FollowerAbortsBeforeServerCall(t *testing.T) {
	stub := &stubServer{
		statusBody: map[string]any{
			"mode":      "cluster",
			"state":     "Follower",
			"leader_id": "n1",
			"peers":     []string{"n1", "n2", "n3"},
		},
	}
	srv := httptest.NewServer(stub.handler())
	defer srv.Close()

	_, err := runRemovePeer(t, srv.URL, []string{"n3", "--yes"}, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not the leader")
	assert.Equal(t, int32(0), stub.removeCalls.Load())
}

func TestCmdRemovePeer_PromptRejectsBlankInput(t *testing.T) {
	stub := &stubServer{
		statusBody: map[string]any{
			"mode":      "cluster",
			"state":     "Leader",
			"leader_id": "n1",
			"peers":     []string{"n1", "n2", "n3"},
		},
	}
	srv := httptest.NewServer(stub.handler())
	defer srv.Close()

	_, err := runRemovePeer(t, srv.URL, []string{"n3"}, "\n")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "aborted")
	assert.Equal(t, int32(0), stub.removeCalls.Load())
}

func TestCmdRemovePeer_ForceFlagPropagates(t *testing.T) {
	stub := &stubServer{
		statusBody: map[string]any{
			"mode":       "cluster",
			"state":      "Leader",
			"leader_id":  "n1",
			"peers":      []string{"n1", "n2", "n3"},
			"down_nodes": []string{"n3"},
		},
		removeBody: map[string]any{"status": "removed", "id": "n2"},
	}
	srv := httptest.NewServer(stub.handler())
	defer srv.Close()

	_, err := runRemovePeer(t, srv.URL, []string{"n2", "--force", "--yes"}, "")
	require.NoError(t, err)
	body := stub.removedBody.Load().(map[string]any)
	assert.Equal(t, true, body["force"])
}

func TestCmdRemovePeer_LocalModeRefuses(t *testing.T) {
	stub := &stubServer{statusBody: map[string]any{"mode": "local"}}
	srv := httptest.NewServer(stub.handler())
	defer srv.Close()

	_, err := runRemovePeer(t, srv.URL, []string{"n3", "--yes"}, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not in cluster mode")
}
