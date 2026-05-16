package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/audit"
	"github.com/gritive/GrainFS/internal/s3auth"
	"github.com/gritive/GrainFS/internal/storage"
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
	require.NoError(t, outbox.AppendFinalized(context.Background(), audit.S3Event{
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

func TestAuditHealthAPIAllowsUnsignedLocalDashboardWithS3AuthEnabled(t *testing.T) {
	outbox, err := audit.OpenOutbox(t.TempDir())
	require.NoError(t, err)
	defer outbox.Close()

	base := setupTestServerWithOptions(t,
		WithAuth([]s3auth.Credentials{{AccessKey: "AK", SecretKey: "secret"}}),
		WithAuditOutbox(outbox),
	)

	resp, err := http.Get(base + "/api/audit/health")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
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
	filter := searcher.Filter()
	require.Equal(t, "b", filter.Bucket)
	require.Equal(t, 400, filter.StatusClass)
	require.Equal(t, 5, filter.Limit)
	require.False(t, filter.Since.IsZero())
}

func TestAuditSearchS3APIClampsLimit(t *testing.T) {
	searcher := &fakeAuditSearcher{}
	base := setupTestServerWithOptions(t, WithAuditSearcher(searcher))

	resp, err := http.Get(base + "/api/audit/s3?limit=10000")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, audit.MaxSearchLimit, searcher.Filter().Limit)
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

func TestAuditSearcherClosedOnShutdown(t *testing.T) {
	backend, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	defer backend.Close()

	searcher := &closingAuditSearcher{}
	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	srv := New(addr, backend, WithAuditSearcher(searcher))
	go srv.Run() //nolint:errcheck
	for i := 0; i < 50; i++ {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			conn.Close()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	require.NoError(t, srv.Shutdown(context.Background()))
	require.True(t, searcher.closed)
}

type fakeAuditSearcher struct {
	mu     sync.Mutex
	filter audit.SearchFilter
	rows   []audit.SearchRow
	err    error
}

func (f *fakeAuditSearcher) SearchS3(_ context.Context, filter audit.SearchFilter) ([]audit.SearchRow, error) {
	f.mu.Lock()
	f.filter = filter
	rows := f.rows
	err := f.err
	f.mu.Unlock()
	return rows, err
}

func (f *fakeAuditSearcher) Filter() audit.SearchFilter {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.filter
}

type closingAuditSearcher struct {
	closed bool
}

func (c *closingAuditSearcher) SearchS3(context.Context, audit.SearchFilter) ([]audit.SearchRow, error) {
	return nil, nil
}

func (c *closingAuditSearcher) Close() error {
	c.closed = true
	return nil
}
