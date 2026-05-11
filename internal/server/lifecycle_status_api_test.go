package server

import (
	"encoding/json"
	"net/http"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var st lifecycle.Status
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&st))
	assert.False(t, st.Running)
}
