package clusteradmin

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordedConfigPatch captures the last PATCH /v1/cluster/config body + the
// If-Match-Rev header against an httptest.Server. Mirrors the cmd-side fixture
// pre-relocation; ours uses httptest so no UDS plumbing is needed.
type recordedConfigPatch struct {
	mu         sync.Mutex
	body       []byte
	ifMatchRev string
}

func (r *recordedConfigPatch) LastBody() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return string(r.body)
}

func (r *recordedConfigPatch) LastIfMatchRev() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.ifMatchRev
}

func sampleClusterConfigResponse() ClusterConfigResponse {
	return ClusterConfigResponse{
		Rev:         7,
		UpdatedAtMs: 1_700_000_000_000,
		Effective: map[string]any{
			"balancer-imbalance-trigger-pct": 25.5,
			"balancer-imbalance-stop-pct":    10.0,
			"alert-webhook":                  "https://x.example",
		},
		Source: map[string]string{
			"balancer-imbalance-trigger-pct": "explicit",
			"balancer-imbalance-stop-pct":    "default",
			"alert-webhook":                  "explicit",
		},
	}
}

func TestClient_ClusterConfigGet_HitsExpectedEndpoint(t *testing.T) {
	var gotMethod, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(sampleClusterConfigResponse())
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	resp, err := c.ClusterConfigGet(context.Background())
	require.NoError(t, err)
	assert.Equal(t, http.MethodGet, gotMethod)
	assert.Equal(t, "/v1/cluster/config", gotPath)
	assert.Equal(t, uint64(7), resp.Rev)
	assert.Equal(t, "explicit", resp.Source["balancer-imbalance-trigger-pct"])
	assert.Equal(t, "https://x.example", resp.Effective["alert-webhook"])
}

func TestClient_ClusterConfigPatch_SendsPatch(t *testing.T) {
	rec := &recordedConfigPatch{}
	var gotMethod, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		body, _ := io.ReadAll(r.Body)
		rec.mu.Lock()
		rec.body = body
		rec.ifMatchRev = r.Header.Get("If-Match-Rev")
		rec.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]uint64{"rev": 8})
	}))
	defer srv.Close()

	trig := 27.5
	c := NewClient(srv.URL)
	rev, err := c.ClusterConfigPatch(
		context.Background(),
		ClusterConfigPatchRequest{BalancerImbalanceTriggerPct: &trig},
		0,
	)
	require.NoError(t, err)
	assert.Equal(t, http.MethodPatch, gotMethod)
	assert.Equal(t, "/v1/cluster/config", gotPath)
	assert.Equal(t, uint64(8), rev)
	assert.JSONEq(t, `{"balancer-imbalance-trigger-pct":27.5}`, rec.LastBody())
	assert.Equal(t, "", rec.LastIfMatchRev())
}

func TestClient_ClusterConfigPatch_IfMatchRev(t *testing.T) {
	rec := &recordedConfigPatch{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		rec.mu.Lock()
		rec.body = body
		rec.ifMatchRev = r.Header.Get("If-Match-Rev")
		rec.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]uint64{"rev": 8})
	}))
	defer srv.Close()

	trig := 27.0
	c := NewClient(srv.URL)
	_, err := c.ClusterConfigPatch(
		context.Background(),
		ClusterConfigPatchRequest{BalancerImbalanceTriggerPct: &trig},
		7,
	)
	require.NoError(t, err)
	assert.Equal(t, "7", rec.LastIfMatchRev())
}

func TestClient_ClusterConfigPatchRaw_FreeForm(t *testing.T) {
	rec := &recordedConfigPatch{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		rec.mu.Lock()
		rec.body = body
		rec.ifMatchRev = r.Header.Get("If-Match-Rev")
		rec.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]uint64{"rev": 11})
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	rev, err := c.ClusterConfigPatchRaw(context.Background(), map[string]any{
		"balancer-imbalance-trigger-pct": 30,
		"alert-webhook":                  "https://x.example",
		"balancer-enabled":               true,
	}, 7)
	require.NoError(t, err)
	assert.Equal(t, uint64(11), rev)
	assert.Equal(t, "7", rec.LastIfMatchRev())
	assert.JSONEq(t, `{
		"balancer-imbalance-trigger-pct":30,
		"alert-webhook":"https://x.example",
		"balancer-enabled":true
	}`, rec.LastBody())
}

func TestClient_ClusterConfigPatch_ResetKeys(t *testing.T) {
	rec := &recordedConfigPatch{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		rec.mu.Lock()
		rec.body = body
		rec.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]uint64{"rev": 9})
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	rev, err := c.ClusterConfigPatch(
		context.Background(),
		ClusterConfigPatchRequest{ResetKeys: []string{"balancer-imbalance-trigger-pct", "alert-webhook"}},
		0,
	)
	require.NoError(t, err)
	assert.Equal(t, uint64(9), rev)
	assert.JSONEq(t,
		`{"reset_keys":["balancer-imbalance-trigger-pct","alert-webhook"]}`,
		rec.LastBody(),
	)
}
