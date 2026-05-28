package serveruntime

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster"
)

func TestRevokeNodeHandler_Unavailable_Returns503(t *testing.T) {
	h := &RevokeNodeHandler{}
	rec := httptest.NewRecorder()
	req, err := http.NewRequest(http.MethodPost, "/v1/cluster/revoke-node", strings.NewReader(`{"node_id":"n2"}`))
	require.NoError(t, err)

	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

func TestRevokeNodeHandler_EmptyNodeID_Returns400(t *testing.T) {
	h := &RevokeNodeHandler{RunRevoke: func(context.Context, string) error { return nil }}
	rec := httptest.NewRecorder()
	req, err := http.NewRequest(http.MethodPost, "/v1/cluster/revoke-node", strings.NewReader(`{"node_id":""}`))
	require.NoError(t, err)

	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestRevokeNodeHandler_NotFound_Returns404(t *testing.T) {
	h := &RevokeNodeHandler{RunRevoke: func(context.Context, string) error {
		return cluster.ErrRevokeNodeNotFound
	}}
	rec := httptest.NewRecorder()
	req, err := http.NewRequest(http.MethodPost, "/v1/cluster/revoke-node", strings.NewReader(`{"node_id":"missing"}`))
	require.NoError(t, err)

	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestRevokeNodeHandler_Success_Returns200(t *testing.T) {
	var got string
	h := &RevokeNodeHandler{RunRevoke: func(_ context.Context, nodeID string) error {
		got = nodeID
		return nil
	}}
	rec := httptest.NewRecorder()
	req, err := http.NewRequest(http.MethodPost, "/v1/cluster/revoke-node", strings.NewReader(`{"node_id":"node-b"}`))
	require.NoError(t, err)

	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "node-b", got)
	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Equal(t, "ok", resp["status"])
}
