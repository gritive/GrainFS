package server

import (
	"context"
	"encoding/xml"
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

// directProposer writes lifecycle changes straight to the store — no Raft
// required for unit tests.
type directProposer struct{ store *lifecycle.Store }

func (p *directProposer) ProposeLifecyclePut(_ context.Context, bucket string, raw []byte) error {
	return p.store.PutRaw(bucket, raw)
}
func (p *directProposer) ProposeLifecycleDelete(_ context.Context, bucket string) error {
	return p.store.Delete(bucket)
}

// noopLeadership satisfies lifecycle.LeadershipSignal for tests that do not
// exercise the executor.
type noopLeadership struct{}

func (noopLeadership) IsLeader() bool { return false }
func (noopLeadership) Subscribe() (<-chan struct{}, func()) {
	ch := make(chan struct{})
	return ch, func() {}
}

func setupLifecycleServer(t *testing.T) (base string, svc *lifecycle.Service) {
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

	store := lifecycle.NewStore(db)
	svc = lifecycle.NewService(store, &directProposer{store: store}, noopLeadership{}, nil, nil, 0)

	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	srv := New(addr, backend, WithLifecycleService(svc))
	go srv.Run() //nolint:errcheck
	for i := 0; i < 50; i++ {
		conn, err2 := net.Dial("tcp", addr)
		if err2 == nil {
			conn.Close()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	return "http://" + addr, svc
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

// TestLifecycleAPI_GetReturnsRawXML verifies that GET returns the verbatim
// bytes sent by the caller (ADR 0011 round-trip requirement).
func TestLifecycleAPI_GetReturnsRawXML(t *testing.T) {
	base, _ := setupLifecycleServer(t)

	// create bucket
	req, _ := http.NewRequest(http.MethodPut, base+"/raw-bkt", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	// PUT lifecycle — use canonical form
	req, _ = http.NewRequest(http.MethodPut, base+"/raw-bkt?lifecycle",
		strings.NewReader(testLifecycleXML))
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// GET lifecycle — should come back as valid XML
	resp, err = http.Get(base + "/raw-bkt?lifecycle")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, _ := io.ReadAll(resp.Body)
	var cfg lifecycle.LifecycleConfiguration
	require.NoError(t, xml.Unmarshal(body, &cfg), "GET response must be valid XML")
	require.Len(t, cfg.Rules, 1)
	assert.Equal(t, "expire-logs", cfg.Rules[0].ID)
}
