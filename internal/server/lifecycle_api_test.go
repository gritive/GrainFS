package server

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/lifecycle"
	"github.com/gritive/GrainFS/internal/storage"
)

func setupLifecycleServer(t *testing.T) (base string, store *lifecycle.Store) {
	t.Helper()
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })

	dbDir := t.TempDir()
	opts := badger.DefaultOptions(dbDir).WithLogger(nil)
	db, err := badger.Open(opts)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	store = lifecycle.NewStore(db)

	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	srv := New(addr, backend, WithLifecycleStore(store))
	go srv.Run() //nolint:errcheck
	for i := 0; i < 50; i++ {
		conn, err2 := net.Dial("tcp", addr)
		if err2 == nil {
			conn.Close()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	return "http://" + addr, store
}

const testLifecycleXML = `<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration>
  <Rule>
    <ID>expire-logs</ID>
    <Status>Enabled</Status>
    <Filter><Prefix>logs/</Prefix></Filter>
    <Expiration><Days>30</Days></Expiration>
  </Rule>
</LifecycleConfiguration>`

func TestLifecycleAPI_GetNotSet(t *testing.T) {
	base, _ := setupLifecycleServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/test-bucket", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp, err = http.Get(base + "/test-bucket?lifecycle")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestLifecycleAPI_PutAndGet(t *testing.T) {
	base, _ := setupLifecycleServer(t)

	// create bucket
	req, _ := http.NewRequest(http.MethodPut, base+"/my-bucket", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	// PUT lifecycle
	req, _ = http.NewRequest(http.MethodPut, base+"/my-bucket?lifecycle",
		strings.NewReader(testLifecycleXML))
	req.Header.Set("Content-Type", "application/xml")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// GET lifecycle
	resp, err = http.Get(base + "/my-bucket?lifecycle")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	body, _ := io.ReadAll(resp.Body)
	assert.Contains(t, string(body), "expire-logs")
	assert.Contains(t, string(body), "<Days>30</Days>")
}

func TestLifecycleAPI_Delete(t *testing.T) {
	base, _ := setupLifecycleServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/bkt", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	// PUT
	req, _ = http.NewRequest(http.MethodPut, base+"/bkt?lifecycle",
		strings.NewReader(testLifecycleXML))
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	// DELETE
	req, _ = http.NewRequest(http.MethodDelete, base+"/bkt?lifecycle", nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)

	// GET → 404
	resp, err = http.Get(base + "/bkt?lifecycle")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestLifecycleAPI_PutInvalidXML(t *testing.T) {
	base, _ := setupLifecycleServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/bkt", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	req, _ = http.NewRequest(http.MethodPut, base+"/bkt?lifecycle",
		strings.NewReader("<not-valid>"))
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}
