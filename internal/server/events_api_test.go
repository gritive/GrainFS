package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net"
	"net/http"
	"runtime"
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

func TestEmitEvent_BoundedQueueNoGoroutineLeak(t *testing.T) {
	// Regression: emitEvent previously spawned one goroutine per event, so a
	// burst of N events produced N live goroutines. The bounded queue + single
	// worker must cap goroutine count regardless of burst size.
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })

	opts := badger.DefaultOptions(t.TempDir()).WithLogger(nil)
	db, err := badger.Open(opts)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	evStore := eventstore.New(db)

	// Construct Server without starting the HTTP listener — this isolates the
	// event worker path from Hertz shutdown complexity.
	srv := &Server{backend: backend, evStore: evStore, hub: NewHub()}
	srv.startEventWorker()
	t.Cleanup(func() { srv.stopEventWorker() })

	before := runtime.NumGoroutine()
	dropsBefore := eventDropsTotal.Load()

	// Emit many more events than the queue can hold. Excess events must be
	// dropped (counted) rather than producing goroutines.
	const burst = eventQueueSize * 3
	for i := 0; i < burst; i++ {
		srv.emitEvent(eventstore.Event{Type: eventstore.EventTypeS3, Action: eventstore.EventActionPut, Bucket: "b", Key: "k"})
	}

	peak := runtime.NumGoroutine()
	assert.Less(t, peak-before, 10, "goroutine count must not scale with event burst (before=%d, peak=%d)", before, peak)

	// Some portion of burst must have been dropped (queue is capacity eventQueueSize).
	drops := eventDropsTotal.Load() - dropsBefore
	assert.Greater(t, drops, uint64(0), "bounded queue must drop excess events")

	// Worker drains on close and stopEventWorker returns promptly.
	start := time.Now()
	srv.stopEventWorker()
	assert.Less(t, time.Since(start), 5*time.Second, "stopEventWorker must drain promptly")
}

func TestFormUpload_EmitsEvent(t *testing.T) {
	base, evStore := setupTestServerWithEvents(t)

	// Create bucket first
	req, _ := http.NewRequest(http.MethodPut, base+"/form-bucket", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	// POST multipart/form-data upload
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	require.NoError(t, w.WriteField("key", "form-object.txt"))
	require.NoError(t, w.WriteField("Content-Type", "text/plain"))
	fw, err := w.CreateFormFile("file", "form-object.txt")
	require.NoError(t, err)
	_, err = fw.Write([]byte("form upload body"))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	req, _ = http.NewRequest(http.MethodPost, base+"/form-bucket", &buf)
	req.Header.Set("Content-Type", w.FormDataContentType())
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	// emitEvent is fire-and-forget; poll the store briefly for the expected Put event.
	var putEvents []eventstore.Event
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		events, err := evStore.Query(time.Now().Add(-time.Hour), time.Now(), 100, nil)
		require.NoError(t, err)
		putEvents = putEvents[:0]
		for _, e := range events {
			if e.Type == eventstore.EventTypeS3 && e.Action == eventstore.EventActionPut && e.Bucket == "form-bucket" && e.Key == "form-object.txt" {
				putEvents = append(putEvents, e)
			}
		}
		if len(putEvents) > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	require.Len(t, putEvents, 1, "form upload must emit exactly one EventActionPut")
	assert.EqualValues(t, len("form upload body"), putEvents[0].Size)
}

func TestQueryEventLog_SinceLargeRelativeOffset(t *testing.T) {
	// Regression: since=86400 (24h) and since=604800 (7d) were being parsed as
	// absolute Unix seconds (→ start-at-1970), which made "Last 24h" actually mean
	// "Last ~56 years." Confirm since is a relative offset and excludes older events.
	base, evStore := setupTestServerWithEvents(t)

	// Event from 3 days ago
	threeDaysAgo := time.Now().Add(-3 * 24 * time.Hour).UnixNano()
	require.NoError(t, evStore.Append(eventstore.Event{Timestamp: threeDaysAgo, Type: eventstore.EventTypeS3, Action: eventstore.EventActionPut, Bucket: "old", Key: "k"}))

	// Event from 1 minute ago
	require.NoError(t, evStore.Append(eventstore.Event{Type: eventstore.EventTypeS3, Action: eventstore.EventActionPut, Bucket: "new", Key: "k"}))

	// since=86400 (24h): must exclude the 3-day-old event
	resp, err := http.Get(base + "/api/eventlog?since=86400")
	require.NoError(t, err)
	var ev24h []eventstore.Event
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&ev24h))
	resp.Body.Close()
	assert.Len(t, ev24h, 1, "since=86400 must return only events from last 24h")
	if len(ev24h) == 1 {
		assert.Equal(t, "new", ev24h[0].Bucket)
	}

	// since=604800 (7d): must include both
	resp, err = http.Get(base + "/api/eventlog?since=604800")
	require.NoError(t, err)
	var ev7d []eventstore.Event
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&ev7d))
	resp.Body.Close()
	assert.Len(t, ev7d, 2, "since=604800 must return events from last 7d")
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
