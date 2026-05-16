package alerts_test

import (
	"context"
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

// stubReceiver returns a test webhook server, a thread-safe recorder, and a
// shared mutex guarding the recorder slice.
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

// startDispatcher starts d and registers a Cleanup that stops it. Keeps tests
// from forgetting Stop and leaking the controller goroutine.
func startDispatcher(t *testing.T, d *alerts.Dispatcher) {
	t.Helper()
	d.Start(context.Background())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = d.Stop(ctx)
	})
}

func TestDispatcher_SendsSlackFormattedJSON(t *testing.T) {
	srv, captured, mu := stubReceiver(t, http.StatusOK)

	d := alerts.NewDispatcher(srv.URL, alerts.Options{}, nil)
	startDispatcher(t, d)
	d.Send(alerts.Alert{
		Type:     "raft_quorum_lost",
		Severity: alerts.SeverityCritical,
		Resource: "cluster-prod",
		Message:  "Raft cluster lost quorum, 1 of 3 nodes responding",
	})
	d.DrainForTest()

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
	startDispatcher(t, d)
	d.Send(alerts.Alert{
		Type:     "disk_full_imminent",
		Severity: alerts.SeverityWarning,
		Resource: "node-2",
		Message:  "92% used at /data",
	})
	d.DrainForTest()

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
	startDispatcher(t, d)
	d.Send(alerts.Alert{Type: "t", Severity: alerts.SeverityWarning, Message: "m"})
	d.DrainForTest()

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
	startDispatcher(t, d)

	a := alerts.Alert{Type: "raft_quorum_lost", Severity: alerts.SeverityCritical, Resource: "cluster", Message: "lost"}
	d.Send(a)
	d.DrainForTest()

	clock.advance(5 * time.Minute) // still inside dedup window
	d.Send(a)
	d.DrainForTest()

	mu.Lock()
	got := len(*captured)
	mu.Unlock()
	assert.Equal(t, 1, got, "second alert within dedup window must be suppressed")

	clock.advance(6 * time.Minute) // 11 min total → outside window
	d.Send(a)
	d.DrainForTest()

	mu.Lock()
	got = len(*captured)
	mu.Unlock()
	assert.Equal(t, 2, got, "alert outside dedup window must be delivered again")
}

func TestDispatcher_DedupKeyDifferentResourceNotSuppressed(t *testing.T) {
	srv, captured, mu := stubReceiver(t, http.StatusOK)

	d := alerts.NewDispatcher(srv.URL, alerts.Options{DedupWindow: 10 * time.Minute}, nil)
	startDispatcher(t, d)
	d.Send(alerts.Alert{Type: "disk_full", Severity: alerts.SeverityWarning, Resource: "node-1", Message: "m"})
	d.Send(alerts.Alert{Type: "disk_full", Severity: alerts.SeverityWarning, Resource: "node-2", Message: "m"})
	d.DrainForTest()

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

	resultCh := make(chan error, 1)
	d := alerts.NewDispatcher(srv.URL, alerts.Options{
		MaxRetries: 5,
		// Compress backoff so the test runs in milliseconds, not 30+ seconds.
		BackoffBase: 1 * time.Millisecond,
		BackoffCap:  5 * time.Millisecond,
		OnResult: func(_ alerts.Alert, err error) {
			resultCh <- err
		},
	}, nil)
	startDispatcher(t, d)

	d.Send(alerts.Alert{Type: "raft_quorum_lost", Severity: alerts.SeverityCritical, Message: "m"})

	select {
	case err := <-resultCh:
		require.Error(t, err, "delivery-failed callback must surface an error after exhaustion")
	case <-time.After(2 * time.Second):
		t.Fatal("OnResult not invoked within 2s")
	}
	assert.Equal(t, int32(6), attempts.Load(), "expected 1 initial attempt + 5 retries")
}

func TestDispatcher_RetriesNotInvokedOn2xx(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	d := alerts.NewDispatcher(srv.URL, alerts.Options{MaxRetries: 5}, nil)
	startDispatcher(t, d)
	d.Send(alerts.Alert{Type: "t", Severity: alerts.SeverityWarning, Message: "m"})
	d.DrainForTest()
	assert.Equal(t, int32(1), attempts.Load())
}

func TestDispatcher_NoURL_NoOp(t *testing.T) {
	// An operator who never set --alert-webhook must still get a working
	// dispatcher; Send is just a no-op (sendCmd's empty-URL branch releases
	// the inflight claim without spawning a worker).
	d := alerts.NewDispatcher("", alerts.Options{}, nil)
	startDispatcher(t, d)
	d.Send(alerts.Alert{Type: "t", Severity: alerts.SeverityCritical, Message: "m"})
	d.DrainForTest() // no worker was spawned; barrier still applies cleanly
}

// fakeAlertCfg satisfies alerts.AlertCfgReader with mutable URL + wrapped
// secret slots for the hot-reload tests.
type fakeAlertCfg struct {
	mu      sync.Mutex
	url     string
	wrapped []byte
}

func (f *fakeAlertCfg) AlertWebhook() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.url
}

func (f *fakeAlertCfg) AlertWebhookSecretWrapped() []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.wrapped
}

func (f *fakeAlertCfg) setURL(s string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.url = s
}

// fakeDecrypter returns plaintext unchanged so tests can pin a known secret
// without spinning up a real Encryptor.
type fakeDecrypter struct {
	plaintext []byte
	err       error
}

func (f *fakeDecrypter) DecryptWithAAD(_, _ []byte) ([]byte, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.plaintext, nil
}

// TestWebhook_HotReload_URL locks in the cluster-config contract: a dispatcher
// built with an empty AlertWebhook() must be a no-op, but as soon as the
// config rotates in a URL, the very next Send must hit that URL. No restart,
// no reconstruction.
func TestWebhook_HotReload_URL(t *testing.T) {
	srv, captured, mu := stubReceiver(t, http.StatusOK)
	cfg := &fakeAlertCfg{}

	d := alerts.NewDispatcherWithConfig(cfg, nil, nil, alerts.Options{}, nil, "test")
	startDispatcher(t, d)

	// Empty URL → Send is a no-op, no request reaches the receiver.
	d.Send(alerts.Alert{Type: "t", Severity: alerts.SeverityWarning, Message: "m1"})
	d.DrainForTest()
	mu.Lock()
	require.Empty(t, *captured, "empty-URL config must produce zero deliveries")
	mu.Unlock()

	// Operator PATCHes cluster-config; the next Send must observe the new URL.
	cfg.setURL(srv.URL)
	d.Send(alerts.Alert{Type: "t2", Severity: alerts.SeverityWarning, Resource: "r", Message: "m2"})
	d.DrainForTest()

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, *captured, 1, "post-PATCH Send must deliver to the live URL")
}

// TestWebhook_HotReload_Secret locks in that the live wrapped secret is read
// (and unwrapped via the SecretDecrypter) on each Send so a secret rotation
// lands without a restart.
func TestWebhook_HotReload_Secret(t *testing.T) {
	srv, captured, mu := stubReceiver(t, http.StatusOK)
	cfg := &fakeAlertCfg{url: srv.URL}
	dec := &fakeDecrypter{plaintext: []byte("rotated-secret")}

	d := alerts.NewDispatcherWithConfig(cfg, dec, []byte("aad"), alerts.Options{}, nil, "test")
	startDispatcher(t, d)

	// No wrapped secret yet → no signature header (matches static empty-secret).
	d.Send(alerts.Alert{Type: "t1", Severity: alerts.SeverityWarning, Resource: "r1", Message: "m"})
	d.DrainForTest()

	// Operator PATCHes the wrapped secret. Decrypter returns "rotated-secret"
	// and the next Send must sign with it.
	cfg.mu.Lock()
	cfg.wrapped = []byte("ciphertext-blob")
	cfg.mu.Unlock()

	d.Send(alerts.Alert{Type: "t2", Severity: alerts.SeverityWarning, Resource: "r2", Message: "m"})
	d.DrainForTest()

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, *captured, 2)

	assert.Empty(t, (*captured)[0].headers.Get("X-Grainfs-Signature"),
		"pre-rotation send must be unsigned")

	gotSig := (*captured)[1].headers.Get("X-Grainfs-Signature")
	require.NotEmpty(t, gotSig, "post-rotation send must be signed")

	mac := hmac.New(sha256.New, []byte("rotated-secret"))
	mac.Write((*captured)[1].body)
	want := hex.EncodeToString(mac.Sum(nil))
	assert.Equal(t, want, gotSig, "signature must use the rotated secret")
}

// TestDispatcher_ConcurrentSameKeyOnlyOneDelivered regression-tests the dedup
// claim under concurrent Send. Under the actor model, both sendCmds reach the
// controller sequentially; the second observes inFlight[key] busy and is
// dropped. The first goroutine's worker runs to completion (we block its HTTP
// handler until release so we can observe attempt counts before drain).
func TestDispatcher_ConcurrentSameKeyOnlyOneDelivered(t *testing.T) {
	release := make(chan struct{})
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		<-release // simulate slow webhook so the inFlight holder stays busy
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	d := alerts.NewDispatcher(srv.URL, alerts.Options{
		// Disable dedup window so ONLY the inFlight claim gates concurrent Send.
		DedupWindow: -1,
		MaxRetries:  0,
		BackoffBase: time.Millisecond,
	}, nil)
	startDispatcher(t, d)

	a := alerts.Alert{
		Type:     "raft_quorum_lost",
		Severity: alerts.SeverityCritical,
		Resource: "cluster-prod",
		Message:  "lost",
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()
			<-start
			d.Send(a)
		}()
	}
	close(start)
	wg.Wait() // both Send returns are immediate (fire-and-forget)

	// At this point both sendCmds are in flight to the controller. The first
	// claim wins, second is dropped by inFlight guard. The first worker is
	// blocked in the HTTP handler — assert it reached the receiver once.
	require.Eventually(t, func() bool { return attempts.Load() == 1 },
		time.Second, 5*time.Millisecond,
		"exactly one of the concurrent sends should reach the webhook")
	// Give the controller a moment to demonstrate the second sendCmd doesn't
	// also spawn — by the time attempts has been 1 for >= one polling
	// interval, the second claim has been processed and rejected.
	time.Sleep(30 * time.Millisecond)
	require.Equal(t, int32(1), attempts.Load(),
		"second concurrent sendCmd must be suppressed by inFlight claim")

	// Release the first delivery and let everything drain.
	close(release)
	d.DrainForTest()

	// After the first delivery releases inFlight, a fresh Send for the same
	// key is permitted (no dedup window since DedupWindow=-1).
	d.Send(a)
	d.DrainForTest()
	assert.Equal(t, int32(2), attempts.Load(),
		"Send after inFlight release must be allowed through")
}

// TestDispatcher_RecordSentOnFailureDedupsOutageStorm locks in the failure-path
// dedup contract: when a webhook receiver returns 5xx, the release records
// lastSent anyway so a repeat page inside the dedup window is suppressed.
// Without this, an outage that keeps returning 5xx would produce webhook spam
// at every retry cycle (defeating dedup exactly when the receiver needs
// backpressure most).
func TestDispatcher_RecordSentOnFailureDedupsOutageStorm(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	clock := newFakeClock(time.Unix(0, 0))
	d := alerts.NewDispatcher(srv.URL, alerts.Options{
		DedupWindow: 10 * time.Minute,
		MaxRetries:  2,
		BackoffBase: time.Millisecond,
		BackoffCap:  2 * time.Millisecond,
		Clock:       clock.Now,
	}, nil)
	startDispatcher(t, d)

	a := alerts.Alert{Type: "disk_full", Severity: alerts.SeverityWarning, Resource: "node-3", Message: "m"}
	d.Send(a)
	d.DrainForTest()
	firstAttempts := attempts.Load()
	require.Equal(t, int32(3), firstAttempts, "1 initial + 2 retries before failure surfaces")

	clock.advance(5 * time.Minute) // still inside dedup window
	d.Send(a)
	d.DrainForTest()
	assert.Equal(t, firstAttempts, attempts.Load(),
		"second Send within dedup window must NOT hit the receiver again")

	clock.advance(6 * time.Minute) // 11 min total → outside window
	d.Send(a)
	d.DrainForTest()
	assert.Greater(t, attempts.Load(), firstAttempts,
		"after dedup window passes, a fresh failing delivery is permitted")
}

func TestDispatcher_BackoffRespectsContext(t *testing.T) {
	// Coverage moved to TestRetryAndDeliver_CtxCancelStopsBackoff in
	// webhook_internal_test.go (T9), which exercises the same ctx-cancel
	// semantics through Stop + workerCancel against a real backoff sleep.
	// This placeholder is intentionally retained as a skip until the plan's
	// later tasks decide whether to delete it.
	t.Skip("superseded by TestRetryAndDeliver_CtxCancelStopsBackoff (internal)")
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
