package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

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

func TestAuditSearchS3API(t *testing.T) {
	searcher := &fakeAuditSearcher{
		rows: []audit.SearchRow{{
			Ts:        time.Unix(100, 0).UTC(),
			RequestID: "req-1",
			Operation: "PutObject",
			Bucket:    "b",
			Key:       "k",
			Status:    403,
		}},
	}
	base := setupTestServerWithOptions(t, WithAuditSearcher(searcher))

	resp, err := http.Get(base + "/api/audit/s3?bucket=b&status_class=400&limit=5")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var got []audit.SearchRow
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	require.Len(t, got, 1)
	require.Equal(t, "req-1", got[0].RequestID)
	require.Equal(t, "b", searcher.filter.Bucket)
	require.Equal(t, 400, searcher.filter.StatusClass)
	require.Equal(t, 5, searcher.filter.Limit)
	require.False(t, searcher.filter.Since.IsZero())
}

func TestAuditSearchS3APIClampsLimit(t *testing.T) {
	searcher := &fakeAuditSearcher{}
	base := setupTestServerWithOptions(t, WithAuditSearcher(searcher))

	resp, err := http.Get(base + "/api/audit/s3?limit=10000")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, audit.MaxSearchLimit, searcher.filter.Limit)
}

func TestAuditSearchS3APIRejectsInvalidStatus(t *testing.T) {
	searcher := &fakeAuditSearcher{}
	base := setupTestServerWithOptions(t, WithAuditSearcher(searcher))

	resp, err := http.Get(base + "/api/audit/s3?status=oops")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestAuditSearchS3APIRunnerError(t *testing.T) {
	searcher := &fakeAuditSearcher{err: errors.New("duckdb unavailable")}
	base := setupTestServerWithOptions(t, WithAuditSearcher(searcher))

	resp, err := http.Get(base + "/api/audit/s3")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	var got map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	require.Equal(t, "duckdb unavailable", got["error"])
}

type fakeAuditSearcher struct {
	filter audit.SearchFilter
	rows   []audit.SearchRow
	err    error
}

func (f *fakeAuditSearcher) SearchS3(_ context.Context, filter audit.SearchFilter) ([]audit.SearchRow, error) {
	f.filter = filter
	return f.rows, f.err
}
