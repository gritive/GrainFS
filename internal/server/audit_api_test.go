package server

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/audit"
)

func TestAuditHealthAPI(t *testing.T) {
	outbox, err := audit.OpenOutbox(t.TempDir())
	require.NoError(t, err)
	defer outbox.Close()

	base := setupTestServerWithOptions(t, WithAuditOutbox(outbox))

	resp, err := http.Get(base + "/api/audit/health")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var got auditHealthResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	require.True(t, got.Enabled)
	require.Equal(t, "ok", got.GuaranteeState)
	require.Equal(t, 0, got.OutboxBacklog)
}

func TestAuditHealthAPIReportsBacklog(t *testing.T) {
	outbox, err := audit.OpenOutbox(t.TempDir())
	require.NoError(t, err)
	defer outbox.Close()
	require.NoError(t, outbox.AppendAttempt(context.Background(), audit.S3Event{
		EventID: "evt-1",
		Ts:      123,
	}))

	base := setupTestServerWithOptions(t, WithAuditOutbox(outbox))

	resp, err := http.Get(base + "/api/audit/health")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var got auditHealthResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	require.True(t, got.Enabled)
	require.Equal(t, "ok", got.GuaranteeState)
	require.Equal(t, 1, got.OutboxBacklog)
	require.Equal(t, int64(123), got.OldestPendingUS)
}
