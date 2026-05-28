package serveruntime

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompleteCutoverHandler_DCut4NotMet_Returns400(t *testing.T) {
	h := &CompleteCutoverHandler{
		RunDrop: func(context.Context) error {
			return fmt.Errorf("RunDropClusterKey: D-cut4 gate not satisfied")
		},
	}
	rec := httptest.NewRecorder()
	req, err := http.NewRequest(http.MethodPost, "/v1/cluster/complete-cutover", strings.NewReader("{}"))
	require.NoError(t, err)

	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Contains(t, resp["error"], "D-cut4")
}

func TestCompleteCutoverHandler_Success_Returns200(t *testing.T) {
	h := &CompleteCutoverHandler{
		RunDrop: func(context.Context) error { return nil },
	}
	rec := httptest.NewRecorder()
	req, err := http.NewRequest(http.MethodPost, "/v1/cluster/complete-cutover", strings.NewReader("{}"))
	require.NoError(t, err)

	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Equal(t, "ok", resp["status"])
}
