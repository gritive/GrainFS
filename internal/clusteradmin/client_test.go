package clusteradmin

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/adminapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClient_Status_ParsesCanonicalShape(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/v1/cluster/status", r.URL.Path)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"mode":        "cluster",
			"node_id":     "n1",
			"state":       "Leader",
			"term":        7,
			"leader_id":   "n1",
			"peers":       []string{"n1", "n2", "n3"},
			"down_nodes":  []string{"n3"},
			"peer_addrs":  map[string]string{"n2": "10.0.0.2:7001"},
			"peer_states": map[string]string{"n2": "configured"},
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
	assert.Equal(t, map[string]string{"n2": "10.0.0.2:7001"}, s.PeerAddrs)
	assert.Equal(t, map[string]string{"n2": "configured"}, s.PeerStates)
}

func TestClientPlacementReport(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/cluster/placement", func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "photos", r.URL.Query().Get("bucket"))
		require.Equal(t, "cat.jpg", r.URL.Query().Get("key"))
		require.Equal(t, "25", r.URL.Query().Get("limit"))
		_, _ = w.Write([]byte(`{"desired_policy_basis":"group_voter_count","bucket":"photos","key":"cat.jpg","object_count":1,"actual_profile_counts":{"4+2":1},"details":[{"bucket":"photos","key":"cat.jpg","version_id":"v1","placement_group_id":"group-1","actual_ec_data":4,"actual_ec_parity":2,"desired_ec_data":4,"desired_ec_parity":2,"layout_state":"current","node_ids":["n1","n2"],"size":12}]}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := NewClient(srv.URL)
	report, err := client.Placement(context.Background(), PlacementOptions{Bucket: "photos", Key: "cat.jpg", Limit: 25})
	require.NoError(t, err)
	require.Equal(t, "group_voter_count", report.DesiredPolicyBasis)
	require.Equal(t, 1, report.ObjectCount)
	require.Equal(t, "current", report.Details[0].LayoutState)
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
	assert.Equal(t, http.StatusConflict, rpe.Status)
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
		case <-time.After(200 * time.Millisecond):
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

// TestClient_UDSDial_RoundTrip verifies end-to-end UDS dial via NewClient
// with a bare path. The handler responds to /v1/cluster/status with
// mode="local"; the client must Status() successfully.
func TestClient_UDSDial_RoundTrip(t *testing.T) {
	d, err := os.MkdirTemp("/tmp", "ca-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(d) })
	sock := filepath.Join(d, "admin.sock")

	ln, err := net.Listen("unix", sock)
	require.NoError(t, err)

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/cluster/status", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"mode": "local"})
	})
	go http.Serve(ln, mux) //nolint:errcheck
	t.Cleanup(func() { _ = ln.Close() })

	c := NewClient(sock)
	s, err := c.Status(context.Background())
	require.NoError(t, err)
	require.Equal(t, "local", s.Mode)
}

// TestClient_StatusRaw_ForwardCompat verifies that StatusRaw returns the
// server response body byte-for-byte. New fields not in the typed Status
// struct are preserved.
func TestClient_StatusRaw_ForwardCompat(t *testing.T) {
	payload := `{"mode":"cluster","node_id":"n1","future_field":{"x":42}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/v1/cluster/status", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(payload))
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	raw, err := c.StatusRaw(context.Background())
	require.NoError(t, err)
	require.Equal(t, payload, string(raw))
}

func TestClient_StatusRaw_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(503)
		_, _ = w.Write([]byte(`{"error":"degraded"}`))
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	_, err := c.StatusRaw(context.Background())
	require.Error(t, err)
	var ae *adminapi.Error
	require.True(t, errors.As(err, &ae), "must surface *adminapi.Error with Status")
	require.Equal(t, 503, ae.Status)
}

func TestClient_EventLog_ParsesArray(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/v1/cluster/eventlog", r.URL.Path)
		// Server expects since/limit query params.
		assert.NotEmpty(t, r.URL.Query().Get("since"))
		assert.NotEmpty(t, r.URL.Query().Get("limit"))
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"ts": 100, "type": "system", "action": "cluster-remove-peer"},
		})
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	events, err := c.EventLog(context.Background(), time.Hour, 50)
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Equal(t, "cluster-remove-peer", events[0].Action)
}

func TestClient_TransferLeader_Happy(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/v1/cluster/transfer-leader", r.URL.Path)
		require.Equal(t, http.MethodPost, r.Method)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"old_leader": "n1",
			"term":       8,
		})
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	res, err := c.TransferLeader(context.Background())
	require.NoError(t, err)
	require.Equal(t, "n1", res.OldLeader)
	require.Equal(t, uint64(8), res.Term)
}

func TestClient_TransferLeader_NotLeader(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error":     "not leader",
			"leader_id": "n2",
		})
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	_, err := c.TransferLeader(context.Background())
	require.Error(t, err)
	var tle *TransferLeaderError
	require.True(t, errors.As(err, &tle))
	require.Equal(t, http.StatusConflict, tle.StatusCode)
	require.Equal(t, "n2", tle.LeaderID)
	require.False(t, tle.Retry)
}

func TestClient_TransferLeader_Race(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error":     "leadership changed during transfer",
			"leader_id": "n3",
			"retry":     true,
		})
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	_, err := c.TransferLeader(context.Background())
	var tle *TransferLeaderError
	require.True(t, errors.As(err, &tle))
	require.True(t, tle.Retry)
	require.Equal(t, "n3", tle.LeaderID)
}

func TestClient_Health_TypedParse(t *testing.T) {
	payload := `{
		"mode":"cluster","degraded":false,"leader_id":"n1","term":7,
		"quorum":{"voters_total":3,"alive_count":3,"required":2,"healthy":true},
		"peers":[{"peer_id":"n1","state":"self"},{"peer_id":"n2","state":"live"}],
		"issues":[]
	}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/v1/cluster/health", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(payload))
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	h, err := c.Health(context.Background())
	require.NoError(t, err)
	require.Equal(t, "cluster", h.Mode)
	require.True(t, h.Quorum.Healthy)
	require.Equal(t, 2, len(h.Peers))
	require.Equal(t, "self", h.Peers[0].State)
}

func TestClient_BalancerStatus_TypedParse(t *testing.T) {
	payload := `{
		"available":true,"active":true,"imbalance_pct":3.4,
		"nodes":[
			{"node_id":"n1","disk_used_pct":84.0,"disk_avail_bytes":107374182400,"requests_per_sec":42}
		]
	}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/v1/cluster/balancer/status", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(payload))
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	b, err := c.BalancerStatus(context.Background())
	require.NoError(t, err)
	require.True(t, b.Available)
	require.True(t, b.Active)
	require.Equal(t, 1, len(b.Nodes))
	require.Equal(t, "n1", b.Nodes[0].NodeID)
}
