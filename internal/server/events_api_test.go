package server

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/eventstore"
	"github.com/gritive/GrainFS/internal/storage"
)

func setupTestServerWithEvents(t *testing.T) (string, *eventstore.Store) {
	t.Helper()
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })

	opts := badger.DefaultOptions(t.TempDir()).WithLogger(nil)
	db, err := badger.Open(opts)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	evStore := eventstore.New(db)

	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	srv := New(addr, backend, WithEventStore(evStore))
	go srv.Run() //nolint:errcheck
	for i := 0; i < 50; i++ {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			conn.Close()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	return "http://" + addr, evStore
}

func TestQueryEventLog_NilStore(t *testing.T) {
	base := setupTestServer(t)
	resp, err := http.Get(base + "/api/eventlog")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var events []eventstore.Event
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&events))
	assert.Empty(t, events)
}

func TestQueryEventLog_EmptyResult(t *testing.T) {
	base, _ := setupTestServerWithEvents(t)
	resp, err := http.Get(base + "/api/eventlog")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var events []eventstore.Event
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&events))
	assert.Empty(t, events)
}

func TestQueryEventLog_WithEvents(t *testing.T) {
	base, evStore := setupTestServerWithEvents(t)

	require.NoError(t, evStore.Append(eventstore.Event{Type: eventstore.EventTypeS3, Action: eventstore.EventActionPut, Bucket: "b", Key: "k"}))
	require.NoError(t, evStore.Append(eventstore.Event{Type: eventstore.EventTypeSystem, Action: eventstore.EventActionSnapshotCreate}))

	resp, err := http.Get(base + "/api/eventlog?since=3600")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var events []eventstore.Event
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&events))
	assert.Len(t, events, 2)
}

func TestQueryEventLog_TypeFilter(t *testing.T) {
	base, evStore := setupTestServerWithEvents(t)

	require.NoError(t, evStore.Append(eventstore.Event{Type: eventstore.EventTypeS3, Action: eventstore.EventActionPut}))
	require.NoError(t, evStore.Append(eventstore.Event{Type: eventstore.EventTypeSystem, Action: eventstore.EventActionSnapshotCreate}))

	resp, err := http.Get(base + "/api/eventlog?type=s3")
	require.NoError(t, err)
	defer resp.Body.Close()
	var events []eventstore.Event
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&events))
	assert.Len(t, events, 1)
	assert.Equal(t, eventstore.EventTypeS3, events[0].Type)
}
