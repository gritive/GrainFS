package clusteradmin

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordedJoin captures the last POST /v1/cluster/join request seen by the
// httptest server. Mirrors the cmd-side fixture pre-relocation; the wire shape
// (method, path, request body, force field) is what we assert.
type recordedJoin struct {
	mu     sync.Mutex
	method string
	path   string
	body   []byte
}

func (r *recordedJoin) snapshot() (string, string, []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.method, r.path, r.body
}

func joinStubHandler(rec *recordedJoin, statusCode int, response any) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		rec.mu.Lock()
		rec.method = r.Method
		rec.path = r.URL.Path
		rec.body = body
		rec.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		code := statusCode
		if code == 0 {
			code = http.StatusOK
		}
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(response)
	}
}

func TestClient_JoinViaUDS_RestartInitiated(t *testing.T) {
	rec := &recordedJoin{}
	srv := httptest.NewServer(joinStubHandler(rec, http.StatusOK, map[string]string{
		"status":  "restart_initiated",
		"message": "node will restart and join 127.0.0.1:8300",
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	resp, err := c.JoinViaUDS(context.Background(), "127.0.0.1:8300", false)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "restart_initiated", resp.Status)
	assert.Contains(t, resp.Message, "127.0.0.1:8300")

	method, path, body := rec.snapshot()
	assert.Equal(t, http.MethodPost, method)
	assert.Equal(t, "/v1/cluster/join", path)
	assert.JSONEq(t, `{"peer_addr":"127.0.0.1:8300"}`, string(body))
}

func TestClient_JoinViaUDS_ForceTrue_PropagatedToServer(t *testing.T) {
	rec := &recordedJoin{}
	srv := httptest.NewServer(joinStubHandler(rec, http.StatusOK, map[string]string{
		"status": "restart_initiated",
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	_, err := c.JoinViaUDS(context.Background(), "127.0.0.1:8300", true)
	require.NoError(t, err)

	_, _, body := rec.snapshot()
	assert.JSONEq(t, `{"peer_addr":"127.0.0.1:8300","force":true}`, string(body))
}

func TestClient_JoinViaUDS_AlreadyMember(t *testing.T) {
	rec := &recordedJoin{}
	srv := httptest.NewServer(joinStubHandler(rec, http.StatusOK, map[string]string{
		"status":  "already_member",
		"message": "node is already part of a multi-node cluster",
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	resp, err := c.JoinViaUDS(context.Background(), "127.0.0.1:8300", false)
	require.NoError(t, err)
	assert.Equal(t, "already_member", resp.Status)
}

func TestClient_JoinViaUDS_DataPresent_409_ReturnsTypedError(t *testing.T) {
	rec := &recordedJoin{}
	srv := httptest.NewServer(joinStubHandler(rec, http.StatusConflict, map[string]string{
		"status":  "data_present",
		"message": "solo node has user data; re-send with force=true to discard it and join",
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	resp, err := c.JoinViaUDS(context.Background(), "127.0.0.1:8300", false)
	require.Error(t, err)
	assert.Nil(t, resp)

	var conflict *JoinConflictError
	require.True(t, errors.As(err, &conflict), "expected *JoinConflictError, got %T: %v", err, err)
	assert.Equal(t, "data_present", conflict.Status)
	assert.Contains(t, conflict.Message, "force=true")
}
