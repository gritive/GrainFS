package server

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/badgerutil"
	"github.com/gritive/GrainFS/internal/lifecycle"
)

func TestLifecycleStatus_NilService_Returns503(t *testing.T) {
	// setupTestServer creates a server without a lifecycle service → enabled: false
	base := setupTestServer(t)

	resp, err := http.Get(base + "/api/cluster/lifecycle/status")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)

	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, false, body["enabled"])
}

func TestLifecycleStatus_WithService_Returns200(t *testing.T) {
	dbDir := t.TempDir()
	opts := badgerutil.SmallOptions(dbDir)
	db, err := badger.Open(opts)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	store := lifecycle.NewStore(db)
	svc := lifecycle.NewService(store, nil, nil, nil, nil, 0)

	base := setupTestServerWithOptions(t, WithLifecycleService(svc))

	resp, err := http.Get(base + "/api/cluster/lifecycle/status")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var st lifecycle.Status
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&st))
	assert.False(t, st.Running)
}

// TestLifecycleStatusAPI_IncludesNewFields asserts the JSON shape produced by
// the admin endpoint exposes the Phase-1 metrics (MPU side, cycle wall clock,
// reclaim breakdown).
func TestLifecycleStatusAPI_IncludesNewFields(t *testing.T) {
	dbDir := t.TempDir()
	opts := badger.DefaultOptions(dbDir).WithLogger(nil)
	db, err := badger.Open(opts)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	store := lifecycle.NewStore(db)
	svc := lifecycle.NewService(store, nil, nil, nil, nil, 0)

	base := setupTestServerWithOptions(t, WithLifecycleService(svc))

	resp, err := http.Get(base + "/api/cluster/lifecycle/status")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	body := string(got)
	assert.Contains(t, body, `"aborted_uploads"`)
	assert.Contains(t, body, `"delete_markers_reclaimed"`)
	assert.Contains(t, body, `"last_cycle_seconds"`)
	assert.Contains(t, body, `"mpu_worker_running"`)
	assert.Contains(t, body, `"buckets"`)
}

// TestLifecycleStatus_JSONShape locks in the Status struct's JSON tags so
// regressions in field renames surface without needing the HTTP harness.
func TestLifecycleStatus_JSONShape(t *testing.T) {
	b, err := json.Marshal(lifecycle.Status{})
	require.NoError(t, err)
	body := string(b)
	assert.Contains(t, body, `"aborted_uploads"`)
	assert.Contains(t, body, `"delete_markers_reclaimed"`)
	assert.Contains(t, body, `"last_cycle_seconds"`)
	assert.Contains(t, body, `"mpu_worker_running"`)
	assert.Contains(t, body, `"buckets"`)
}
