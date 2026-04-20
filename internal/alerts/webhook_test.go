package alerts_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/alerts"
)

// receivedRequest captures one inbound webhook delivery for assertions.
type receivedRequest struct {
	headers http.Header
	body    []byte
}

// stubReceiver returns a test webhook server and a thread-safe recorder.
func stubReceiver(t *testing.T, status int) (*httptest.Server, *[]receivedRequest, *sync.Mutex) {
	t.Helper()
	var (
		mu       sync.Mutex
		captured []receivedRequest
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		captured = append(captured, receivedRequest{headers: r.Header.Clone(), body: body})
		mu.Unlock()
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	return srv, &captured, &mu
}

func TestDispatcher_SendsSlackFormattedJSON(t *testing.T) {
	srv, captured, mu := stubReceiver(t, http.StatusOK)

	d := alerts.NewDispatcher(srv.URL, alerts.Options{}, nil)
	require.NoError(t, d.Send(alerts.Alert{
		Type:       "raft_quorum_lost",
		Severity:   alerts.SeverityCritical,
		Resource:   "cluster-prod",
		Message:    "Raft cluster lost quorum, 1 of 3 nodes responding",
	}))

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, *captured, 1)
	req := (*captured)[0]

	var body map[string]any
	require.NoError(t, json.Unmarshal(req.body, &body))
	assert.Contains(t, body, "text", "slack message must have a text field")
	text, _ := body["text"].(string)
	assert.Contains(t, text, "raft_quorum_lost", "alert type must appear in message")
	assert.Contains(t, text, "Raft cluster lost quorum", "human message must appear")
	assert.Contains(t, text, "critical", "severity must appear in message")
}

func TestDispatcher_HMACSignatureWhenSecretSet(t *testing.T) {
	srv, captured, mu := stubReceiver(t, http.StatusOK)

	const secret = "shared-with-receiver"
	d := alerts.NewDispatcher(srv.URL, alerts.Options{Secret: secret}, nil)
	require.NoError(t, d.Send(alerts.Alert{
		Type:     "disk_full_imminent",
		Severity: alerts.SeverityWarning,
		Resource: "node-2",
		Message:  "92% used at /data",
	}))

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, *captured, 1)

	gotSig := (*captured)[0].headers.Get("X-Grainfs-Signature")
	require.NotEmpty(t, gotSig, "signature header missing")

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write((*captured)[0].body)
	want := hex.EncodeToString(mac.Sum(nil))
	assert.Equal(t, want, gotSig, "HMAC mismatch — receiver cannot verify origin")
}

func TestDispatcher_NoSignatureHeaderWhenSecretEmpty(t *testing.T) {
	srv, captured, mu := stubReceiver(t, http.StatusOK)

	d := alerts.NewDispatcher(srv.URL, alerts.Options{}, nil)
	require.NoError(t, d.Send(alerts.Alert{Type: "t", Severity: alerts.SeverityWarning, Message: "m"}))

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, *captured, 1)
	assert.Empty(t, (*captured)[0].headers.Get("X-Grainfs-Signature"))
}

func TestDispatcher_DedupSuppressesWithinWindow(t *testing.T) {
	srv, captured, mu := stubReceiver(t, http.StatusOK)

	clock := newFakeClock(time.Unix(0, 0))
	d := alerts.NewDispatcher(srv.URL, alerts.Options{
		DedupWindow: 10 * time.Minute,
		Clock:       clock.Now,
	}, nil)

	a := alerts.Alert{Type: "raft_quorum_lost", Severity: alerts.SeverityCritical, Resource: "cluster", Message: "lost"}
	require.NoError(t, d.Send(a))

	clock.advance(5 * time.Minute) // still inside dedup window
	require.NoError(t, d.Send(a))

	mu.Lock()
	got := len(*captured)
	mu.Unlock()
	assert.Equal(t, 1, got, "second alert within dedup window must be suppressed")

	clock.advance(6 * time.Minute) // 11 min total → outside window
	require.NoError(t, d.Send(a))

	mu.Lock()
	got = len(*captured)
	mu.Unlock()
	assert.Equal(t, 2, got, "alert outside dedup window must be delivered again")
}

func TestDispatcher_DedupKeyDifferentResourceNotSuppressed(t *testing.T) {
	srv, captured, mu := stubReceiver(t, http.StatusOK)

	d := alerts.NewDispatcher(srv.URL, alerts.Options{DedupWindow: 10 * time.Minute}, nil)
	require.NoError(t, d.Send(alerts.Alert{Type: "disk_full", Severity: alerts.SeverityWarning, Resource: "node-1", Message: "m"}))
	require.NoError(t, d.Send(alerts.Alert{Type: "disk_full", Severity: alerts.SeverityWarning, Resource: "node-2", Message: "m"}))

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 2, len(*captured), "different resources must not dedupe against each other")
}

func TestDispatcher_RetriesWithBackoffThenSurfacesFailure(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	var failed []alerts.Alert
	d := alerts.NewDispatcher(srv.URL, alerts.Options{
		MaxRetries: 5,
		// Compress backoff so the test runs in milliseconds, not 30+ seconds.
		BackoffBase: 1 * time.Millisecond,
		BackoffCap:  5 * time.Millisecond,
	}, func(a alerts.Alert, err error) {
		failed = append(failed, a)
	})

	a := alerts.Alert{Type: "raft_quorum_lost", Severity: alerts.SeverityCritical, Message: "m"}
	err := d.Send(a)
	require.Error(t, err, "5xx exhaustion must surface as an error")

	assert.Equal(t, int32(6), attempts.Load(), "expected 1 initial attempt + 5 retries")
	require.Len(t, failed, 1, "delivery-failed callback must fire once after exhaustion")
	assert.Equal(t, "raft_quorum_lost", failed[0].Type)
}

func TestDispatcher_RetriesNotInvokedOn2xx(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	d := alerts.NewDispatcher(srv.URL, alerts.Options{MaxRetries: 5}, nil)
	require.NoError(t, d.Send(alerts.Alert{Type: "t", Severity: alerts.SeverityWarning, Message: "m"}))
	assert.Equal(t, int32(1), attempts.Load())
}

func TestDispatcher_NoURL_NoOp(t *testing.T) {
	// An operator who never set --alert-webhook must still get a working
	// dispatcher; Send is just a no-op.
	d := alerts.NewDispatcher("", alerts.Options{}, nil)
	assert.NoError(t, d.Send(alerts.Alert{Type: "t", Severity: alerts.SeverityCritical, Message: "m"}))
}

// fakeClock lets dedup tests advance time deterministically.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(t time.Time) *fakeClock {
	return &fakeClock{now: t}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}
