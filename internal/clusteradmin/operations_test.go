package clusteradmin

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubServer fakes /api/cluster/status, /api/cluster/remove-peer, and
// /api/eventlog so RemovePeer/Peers/Events can be exercised end-to-end
// without a running grainfs.
type stubServer struct {
	statusBody  map[string]any
	removeCode  int
	removeBody  map[string]any
	removeCalls atomic.Int32
	removedBody atomic.Value // map[string]any
	eventsBody  []map[string]any
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
	mux.HandleFunc("/api/eventlog", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(s.eventsBody)
	})
	return mux
}

func testPeerSnapshot(rows ...map[string]any) []map[string]any {
	return rows
}

func testLivePeer(id string) map[string]any {
	state := "resolved"
	reason := "raft_append_success"
	if id == "n1" {
		state = "self"
		reason = "self"
	}
	return map[string]any{
		"peer_id":        id,
		"identity_state": state,
		"liveness_state": "live",
		"reason":         reason,
	}
}

func testDownPeer(id string) map[string]any {
	return map[string]any{
		"peer_id":        id,
		"identity_state": "resolved",
		"liveness_state": "health_cooldown",
		"reason":         "peer_health_cooldown",
	}
}

func TestRemovePeer_HappyPath(t *testing.T) {
	stub := &stubServer{
		statusBody: map[string]any{
			"mode":      "cluster",
			"node_id":   "n1",
			"state":     "Leader",
			"leader_id": "n1",
			"peers":     []string{"n2", "n3"},
			"peer_snapshot": testPeerSnapshot(
				testLivePeer("n1"),
				testLivePeer("n2"),
				testLivePeer("n3"),
			),
		},
		removeBody: map[string]any{"status": "removed", "id": "n3"},
	}
	srv := httptest.NewServer(stub.handler())
	defer srv.Close()

	stdout, stderr := &bytes.Buffer{}, &bytes.Buffer{}
	err := RemovePeer(context.Background(), RemovePeerOptions{
		Endpoint:  srv.URL,
		ID:        "n3",
		AssumeYes: true,
		Timeout:   5 * time.Second,
		Stdout:    stdout,
		Stderr:    stderr,
		Stdin:     strings.NewReader(""),
	})
	require.NoError(t, err)
	assert.Contains(t, stdout.String(), "removed n3")
	assert.Contains(t, stderr.String(), "voters: 3 -> 2")
	assert.Equal(t, int32(1), stub.removeCalls.Load())
}

func TestRemovePeer_NonPositiveTimeoutRefused(t *testing.T) {
	for _, d := range []time.Duration{0, -1 * time.Second} {
		err := RemovePeer(context.Background(), RemovePeerOptions{
			Endpoint: "http://127.0.0.1:1",
			ID:       "n3",
			Timeout:  d,
			Stdout:   &bytes.Buffer{},
			Stderr:   &bytes.Buffer{},
		})
		require.Error(t, err, "timeout=%s must be rejected", d)
		assert.Contains(t, err.Error(), "timeout must be positive")
	}
}

func TestRemovePeer_LocalModeRefusedBeforeNetwork(t *testing.T) {
	stub := &stubServer{statusBody: map[string]any{"mode": "local"}}
	srv := httptest.NewServer(stub.handler())
	defer srv.Close()

	err := RemovePeer(context.Background(), RemovePeerOptions{
		Endpoint:  srv.URL,
		ID:        "n3",
		AssumeYes: true,
		Timeout:   5 * time.Second,
		Stdout:    &bytes.Buffer{},
		Stderr:    &bytes.Buffer{},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not in cluster mode")
	assert.Equal(t, int32(0), stub.removeCalls.Load(), "must not POST when local mode")
}

func TestRemovePeer_FollowerAbortedClientSide(t *testing.T) {
	stub := &stubServer{
		statusBody: map[string]any{
			"mode":      "cluster",
			"state":     "Follower",
			"leader_id": "n1",
			"peers":     []string{"n1", "n3"},
		},
	}
	srv := httptest.NewServer(stub.handler())
	defer srv.Close()

	err := RemovePeer(context.Background(), RemovePeerOptions{
		Endpoint:  srv.URL,
		ID:        "n3",
		AssumeYes: true,
		Timeout:   5 * time.Second,
		Stdout:    &bytes.Buffer{},
		Stderr:    &bytes.Buffer{},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not the leader")
	assert.Equal(t, int32(0), stub.removeCalls.Load())
}

func TestRemovePeer_UnknownPeerAbortedClientSide(t *testing.T) {
	stub := &stubServer{
		statusBody: map[string]any{
			"mode":      "cluster",
			"state":     "Leader",
			"leader_id": "n1",
			"peers":     []string{"n2", "n3"},
		},
	}
	srv := httptest.NewServer(stub.handler())
	defer srv.Close()

	err := RemovePeer(context.Background(), RemovePeerOptions{
		Endpoint:  srv.URL,
		ID:        "n99",
		AssumeYes: true,
		Timeout:   5 * time.Second,
		Stdout:    &bytes.Buffer{},
		Stderr:    &bytes.Buffer{},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a current voter")
	assert.Equal(t, int32(0), stub.removeCalls.Load())
}

func TestRemovePeer_PreflightBlocksWithoutForce(t *testing.T) {
	stub := &stubServer{
		statusBody: map[string]any{
			"mode":       "cluster",
			"node_id":    "n1",
			"state":      "Leader",
			"leader_id":  "n1",
			"peers":      []string{"n2", "n3"},
			"down_nodes": []string{"n3"}, // n3 dead → removing n2 leaves only self alive
			"peer_snapshot": testPeerSnapshot(
				testLivePeer("n1"),
				testLivePeer("n2"),
				testDownPeer("n3"),
			),
		},
	}
	srv := httptest.NewServer(stub.handler())
	defer srv.Close()

	err := RemovePeer(context.Background(), RemovePeerOptions{
		Endpoint:  srv.URL,
		ID:        "n2",
		AssumeYes: true,
		Timeout:   5 * time.Second,
		Stdout:    &bytes.Buffer{},
		Stderr:    &bytes.Buffer{},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "alive_after")
	assert.Equal(t, int32(0), stub.removeCalls.Load())
}

func TestRemovePeer_ConfiguredPeerDoesNotCountAsAlive(t *testing.T) {
	stub := &stubServer{
		statusBody: map[string]any{
			"mode":      "cluster",
			"node_id":   "n1",
			"state":     "Leader",
			"leader_id": "n1",
			"peers":     []string{"n2", "n3"},
			"peer_snapshot": []map[string]any{
				{"peer_id": "n1", "identity_state": "self", "liveness_state": "live"},
				{"peer_id": "n2", "identity_state": "resolved", "liveness_state": "configured"},
				{"peer_id": "n3", "identity_state": "resolved", "liveness_state": "live"},
			},
		},
	}
	srv := httptest.NewServer(stub.handler())
	defer srv.Close()

	err := RemovePeer(context.Background(), RemovePeerOptions{
		Endpoint:  srv.URL,
		ID:        "n3",
		AssumeYes: true,
		Timeout:   5 * time.Second,
		Stdout:    &bytes.Buffer{},
		Stderr:    &bytes.Buffer{},
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "alive_after")
	assert.Equal(t, int32(0), stub.removeCalls.Load())
}

func TestRemovePeer_ForceDoesNotBypassUnresolvedLegacyBlocker(t *testing.T) {
	stub := &stubServer{
		statusBody: map[string]any{
			"mode":      "cluster",
			"node_id":   "n1",
			"state":     "Leader",
			"leader_id": "n1",
			"peers":     []string{"n2", "10.0.0.9:7001"},
			"peer_snapshot": []map[string]any{
				{"peer_id": "n1", "identity_state": "self", "liveness_state": "live"},
				{"peer_id": "n2", "identity_state": "resolved", "liveness_state": "live"},
				{"peer_id": "10.0.0.9:7001", "identity_state": "unresolved_legacy", "liveness_state": "configured"},
			},
		},
	}
	srv := httptest.NewServer(stub.handler())
	defer srv.Close()

	err := RemovePeer(context.Background(), RemovePeerOptions{
		Endpoint:  srv.URL,
		ID:        "n2",
		Force:     true,
		AssumeYes: true,
		Timeout:   5 * time.Second,
		Stdout:    &bytes.Buffer{},
		Stderr:    &bytes.Buffer{},
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "identity unresolved")
	assert.Equal(t, int32(0), stub.removeCalls.Load())
}

func TestRemovePeer_ForcePropagatesToServer(t *testing.T) {
	stub := &stubServer{
		statusBody: map[string]any{
			"mode":       "cluster",
			"node_id":    "n1",
			"state":      "Leader",
			"leader_id":  "n1",
			"peers":      []string{"n2", "n3"},
			"down_nodes": []string{"n3"},
			"peer_snapshot": testPeerSnapshot(
				testLivePeer("n1"),
				testLivePeer("n2"),
				testDownPeer("n3"),
			),
		},
		removeBody: map[string]any{"status": "removed", "id": "n2"},
	}
	srv := httptest.NewServer(stub.handler())
	defer srv.Close()

	err := RemovePeer(context.Background(), RemovePeerOptions{
		Endpoint:  srv.URL,
		ID:        "n2",
		Force:     true,
		AssumeYes: true,
		Timeout:   5 * time.Second,
		Stdout:    &bytes.Buffer{},
		Stderr:    &bytes.Buffer{},
	})
	require.NoError(t, err)
	body := stub.removedBody.Load().(map[string]any)
	assert.Equal(t, true, body["force"])
}

func TestRemovePeer_PromptYesProceedsBlankAborts(t *testing.T) {
	stub := &stubServer{
		statusBody: map[string]any{
			"mode":      "cluster",
			"node_id":   "n1",
			"state":     "Leader",
			"leader_id": "n1",
			"peers":     []string{"n2", "n3"},
			"peer_snapshot": testPeerSnapshot(
				testLivePeer("n1"),
				testLivePeer("n2"),
				testLivePeer("n3"),
			),
		},
		removeBody: map[string]any{"status": "removed", "id": "n3"},
	}
	srv := httptest.NewServer(stub.handler())
	defer srv.Close()

	// "y\n" proceeds.
	err := RemovePeer(context.Background(), RemovePeerOptions{
		Endpoint: srv.URL,
		ID:       "n3",
		Timeout:  5 * time.Second,
		Stdout:   &bytes.Buffer{},
		Stderr:   &bytes.Buffer{},
		Stdin:    strings.NewReader("y\n"),
	})
	require.NoError(t, err)
	require.Equal(t, int32(1), stub.removeCalls.Load())

	// blank line aborts (default no).
	err = RemovePeer(context.Background(), RemovePeerOptions{
		Endpoint: srv.URL,
		ID:       "n3",
		Timeout:  5 * time.Second,
		Stdout:   &bytes.Buffer{},
		Stderr:   &bytes.Buffer{},
		Stdin:    strings.NewReader("\n"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "aborted")
	require.Equal(t, int32(1), stub.removeCalls.Load(), "blank input must NOT trigger a second POST")
}

func TestRemovePeer_ServerErrorSurfaced(t *testing.T) {
	stub := &stubServer{
		statusBody: map[string]any{
			"mode":      "cluster",
			"node_id":   "n1",
			"state":     "Leader",
			"leader_id": "n1",
			"peers":     []string{"n2", "n3"},
			"peer_snapshot": testPeerSnapshot(
				testLivePeer("n1"),
				testLivePeer("n2"),
				testLivePeer("n3"),
			),
		},
		removeCode: http.StatusInternalServerError,
		removeBody: map[string]any{"error": "conf change in progress"},
	}
	srv := httptest.NewServer(stub.handler())
	defer srv.Close()

	err := RemovePeer(context.Background(), RemovePeerOptions{
		Endpoint:  srv.URL,
		ID:        "n3",
		AssumeYes: true,
		Timeout:   5 * time.Second,
		Stdout:    &bytes.Buffer{},
		Stderr:    &bytes.Buffer{},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "conf change in progress")
}

func TestPeers_TextRendersAndJSONPassesThrough(t *testing.T) {
	stub := &stubServer{
		statusBody: map[string]any{
			"mode":       "cluster",
			"leader_id":  "n1",
			"peers":      []string{"n2", "n3"},
			"down_nodes": []string{"n3"},
		},
	}
	srv := httptest.NewServer(stub.handler())
	defer srv.Close()

	// Text format renders the table.
	out := &bytes.Buffer{}
	require.NoError(t, Peers(context.Background(), PeersOptions{
		Endpoint: srv.URL,
		Format:   "text",
		Timeout:  5 * time.Second,
		Stdout:   out,
	}))
	rendered := out.String()
	assert.Contains(t, rendered, "ID")
	assert.Contains(t, rendered, "down")

	// JSON format must emit the parsed Status as JSON.
	out.Reset()
	require.NoError(t, Peers(context.Background(), PeersOptions{
		Endpoint: srv.URL,
		Format:   "json",
		Timeout:  5 * time.Second,
		Stdout:   out,
	}))
	var parsed map[string]any
	require.NoError(t, json.Unmarshal(out.Bytes(), &parsed))
	assert.Equal(t, "cluster", parsed["mode"])
}

func TestPeers_LocalModeShortCircuits(t *testing.T) {
	stub := &stubServer{statusBody: map[string]any{"mode": "local"}}
	srv := httptest.NewServer(stub.handler())
	defer srv.Close()

	out := &bytes.Buffer{}
	require.NoError(t, Peers(context.Background(), PeersOptions{
		Endpoint: srv.URL,
		Format:   "text",
		Timeout:  5 * time.Second,
		Stdout:   out,
	}))
	assert.Contains(t, out.String(), "local")
}

func TestEvents_RendersAndAppliesFilter(t *testing.T) {
	stub := &stubServer{
		eventsBody: []map[string]any{
			{"ts": 1, "type": "system", "action": "cluster-join"},
			{"ts": 2, "type": "system", "action": "cluster-remove-peer", "metadata": map[string]any{"removed_id": "n3"}},
			{"ts": 3, "type": "s3", "action": "create-bucket", "bucket": "logs"},
		},
	}
	srv := httptest.NewServer(stub.handler())
	defer srv.Close()

	// Unfiltered: all 3 events present.
	out := &bytes.Buffer{}
	require.NoError(t, Events(context.Background(), EventsOptions{
		Endpoint: srv.URL,
		Since:    time.Hour,
		Limit:    100,
		Format:   "text",
		Timeout:  5 * time.Second,
		Stdout:   out,
	}))
	rendered := out.String()
	assert.Contains(t, rendered, "cluster-join")
	assert.Contains(t, rendered, "cluster-remove-peer")
	assert.Contains(t, rendered, "create-bucket")

	// Filter on cluster-remove-peer only.
	out.Reset()
	require.NoError(t, Events(context.Background(), EventsOptions{
		Endpoint:    srv.URL,
		Since:       time.Hour,
		Limit:       100,
		TypeFilters: []string{"cluster-remove-peer"},
		Format:      "text",
		Timeout:     5 * time.Second,
		Stdout:      out,
	}))
	filtered := out.String()
	assert.Contains(t, filtered, "cluster-remove-peer")
	assert.NotContains(t, filtered, "cluster-join")
	assert.NotContains(t, filtered, "create-bucket")
}
