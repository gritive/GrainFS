package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestExpandPlacementHandler_NotWired proves the handler returns 503 when no
// ExpandPlacementFunc is injected (single-node / cluster mode not configured).
func TestExpandPlacementHandler_NotWired(t *testing.T) {
	sock := udsTestSocket(t)
	cli := startUDSAdminTestServerWithSrv(t, sock, &Server{})

	resp, err := cli.Post("http://unix/v1/cluster/expand-placement", "application/json", nil)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

// TestExpandPlacementHandler_RecordsGeneration proves the handler invokes the
// injected closure and returns its result as JSON (200). The closure stands in
// for serveruntime's coordinator+meta-raft activation.
func TestExpandPlacementHandler_RecordsGeneration(t *testing.T) {
	sock := udsTestSocket(t)
	called := false
	s := &Server{expandPlacement: func(_ context.Context) (ExpandPlacementResult, error) {
		called = true
		return ExpandPlacementResult{
			Base:     []string{"group-1", "group-2"},
			Expanded: []string{"group-1", "group-2", "group-3", "group-4"},
			Added:    []string{"group-3", "group-4"},
			NoOp:     false,
		}, nil
	}}
	cli := startUDSAdminTestServerWithSrv(t, sock, s)

	resp, err := cli.Post("http://unix/v1/cluster/expand-placement", "application/json", nil)
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	require.True(t, called, "handler must invoke the expand-placement closure")

	var out ExpandPlacementResult
	require.NoError(t, json.Unmarshal(body, &out))
	require.False(t, out.NoOp)
	require.Equal(t, []string{"group-3", "group-4"}, out.Added)
	require.Equal(t, []string{"group-1", "group-2", "group-3", "group-4"}, out.Expanded)
}

// TestExpandPlacementHandler_NoOp proves a no-op plan (no new groups) returns
// 200 with no_op=true so the operator sees nothing was recorded.
func TestExpandPlacementHandler_NoOp(t *testing.T) {
	sock := udsTestSocket(t)
	s := &Server{expandPlacement: func(_ context.Context) (ExpandPlacementResult, error) {
		return ExpandPlacementResult{Base: []string{"group-1"}, Expanded: []string{"group-1"}, NoOp: true}, nil
	}}
	cli := startUDSAdminTestServerWithSrv(t, sock, s)

	resp, err := cli.Post("http://unix/v1/cluster/expand-placement", "application/json", nil)
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var out ExpandPlacementResult
	require.NoError(t, json.Unmarshal(body, &out))
	require.True(t, out.NoOp)
}

func TestRetirePlacementGenerationHandler_NotWired(t *testing.T) {
	sock := udsTestSocket(t)
	cli := startUDSAdminTestServerWithSrv(t, sock, &Server{})

	resp, err := cli.Post("http://unix/v1/cluster/retire-placement-generation", "application/json", nil)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

func TestRetirePlacementGenerationHandler_RetiresEpoch(t *testing.T) {
	sock := udsTestSocket(t)
	var gotEpoch uint64
	s := &Server{retirePlacementGeneration: func(_ context.Context, epoch uint64) (RetirePlacementGenerationResult, error) {
		gotEpoch = epoch
		return RetirePlacementGenerationResult{Epoch: epoch, Retired: true}, nil
	}}
	cli := startUDSAdminTestServerWithSrv(t, sock, s)

	resp, err := cli.Post("http://unix/v1/cluster/retire-placement-generation", "application/json",
		strings.NewReader(`{"epoch":7}`))
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	require.Equal(t, uint64(7), gotEpoch)

	var out RetirePlacementGenerationResult
	require.NoError(t, json.Unmarshal(body, &out))
	require.True(t, out.Retired)
	require.Equal(t, uint64(7), out.Epoch)
}
