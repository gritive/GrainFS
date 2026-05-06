package clusteradmin

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClient_Status_ParsesCanonicalShape(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/cluster/status", r.URL.Path)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"mode":       "cluster",
			"node_id":    "n1",
			"state":      "Leader",
			"term":       7,
			"leader_id":  "n1",
			"peers":      []string{"n1", "n2", "n3"},
			"down_nodes": []string{"n3"},
		})
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	s, err := c.Status(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "cluster", s.Mode)
	assert.Equal(t, "Leader", s.State)
	assert.Equal(t, []string{"n1", "n2", "n3"}, s.Peers)
	assert.Equal(t, []string{"n3"}, s.DownNodes)
}

func TestClient_RemovePeer_HappyPath_PostsExpectedBody(t *testing.T) {
	var calls atomic.Int32
	var lastBody atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		body, _ := io.ReadAll(r.Body)
		parsed := map[string]any{}
		_ = json.Unmarshal(body, &parsed)
		lastBody.Store(parsed)
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "removed", "id": "n3"})
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	require.NoError(t, c.RemovePeer(context.Background(), "n3", false))
	assert.Equal(t, int32(1), calls.Load())
	body := lastBody.Load().(map[string]any)
	assert.Equal(t, "n3", body["id"])
	assert.Equal(t, false, body["force"])
}

func TestClient_RemovePeer_NotLeader_ReturnsStructuredError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error":     "not leader",
			"leader_id": "n1",
		})
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	err := c.RemovePeer(context.Background(), "n3", false)
	require.Error(t, err)

	var rpe *RemovePeerError
	require.True(t, errors.As(err, &rpe), "must be *RemovePeerError so callers can branch on status code")
	assert.Equal(t, http.StatusConflict, rpe.StatusCode)
	assert.Equal(t, "n1", rpe.LeaderID)
	assert.Equal(t, "not leader", rpe.Message)
}

func TestClient_RemovePeer_QuorumBlock_CarriesNumbers(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error":        "quorum would break",
			"voters_after": 2,
			"alive_after":  1,
			"new_quorum":   2,
		})
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	err := c.RemovePeer(context.Background(), "n2", false)
	var rpe *RemovePeerError
	require.True(t, errors.As(err, &rpe))
	assert.Equal(t, 2, rpe.VotersAfter)
	assert.Equal(t, 1, rpe.AliveAfter)
	assert.Equal(t, 2, rpe.NewQuorum)
}

func TestClient_RemovePeer_ContextCancelled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(2 * time.Second):
		case <-r.Context().Done():
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := c.RemovePeer(ctx, "n3", false)
	require.Error(t, err, "context deadline must surface")
}

func TestClient_EventLog_ParsesArray(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/eventlog", r.URL.Path)
		// Server expects since/limit query params.
		assert.NotEmpty(t, r.URL.Query().Get("since"))
		assert.NotEmpty(t, r.URL.Query().Get("limit"))
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"ts": 100, "type": "system", "action": "cluster-remove-peer", "metadata": map[string]any{"removed_id": "n3"}},
		})
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	events, err := c.EventLog(context.Background(), time.Hour, 50)
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Equal(t, "cluster-remove-peer", events[0].Action)
	assert.Equal(t, "n3", events[0].Metadata["removed_id"])
}
