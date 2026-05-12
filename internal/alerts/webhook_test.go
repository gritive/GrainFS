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
		Type:     "raft_quorum_lost",
		Severity: alerts.SeverityCritical,
		Resource: "cluster-prod",
		Message:  "Raft cluster lost quorum, 1 of 3 nodes responding",
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

	// Empty URL → Send is a no-op, no request reaches the receiver.
	require.NoError(t, d.Send(alerts.Alert{Type: "t", Severity: alerts.SeverityWarning, Message: "m1"}))
	mu.Lock()
	require.Empty(t, *captured, "empty-URL config must produce zero deliveries")
	mu.Unlock()

	// Operator PATCHes cluster-config; the next Send must observe the new URL.
	cfg.setURL(srv.URL)
	require.NoError(t, d.Send(alerts.Alert{Type: "t2", Severity: alerts.SeverityWarning, Resource: "r", Message: "m2"}))

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

	// No wrapped secret yet → no signature header (matches static empty-secret).
	require.NoError(t, d.Send(alerts.Alert{Type: "t1", Severity: alerts.SeverityWarning, Resource: "r1", Message: "m"}))

	// Operator PATCHes the wrapped secret. Decrypter returns "rotated-secret"
	// and the next Send must sign with it.
	cfg.mu.Lock()
	cfg.wrapped = []byte("ciphertext-blob")
	cfg.mu.Unlock()

	require.NoError(t, d.Send(alerts.Alert{Type: "t2", Severity: alerts.SeverityWarning, Resource: "r2", Message: "m"}))

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
// race that existed before the inFlight set landed: shouldSuppress → unlock →
// HTTP retry → recordSent left a wide window where a second goroutine calling
// Send() with the same (Type, Resource) could also pass the suppression check
// and fire an independent HTTP request. Real-world symptom: duplicate webhook
// pages for the same flapping condition.
//
// Under the inFlight claim, the first goroutine owns the key for the whole
// delivery lifecycle; the second goroutine's claimSend returns false and Send
// becomes a no-op.
func TestDispatcher_ConcurrentSameKeyOnlyOneDelivered(t *testing.T) {
	// Barrier channel lets us deterministically force both goroutines to race
	// on claimSend. The receiver blocks the first delivery in-flight until we
	// release it, which is well after the second goroutine has exited Send.
	release := make(chan struct{})
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		<-release // simulate slow webhook so the inFlight holder has work to do
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	d := alerts.NewDispatcher(srv.URL, alerts.Options{
		// Disable dedup window so ONLY the inFlight claim gates concurrent Send.
		DedupWindow: -1,
		MaxRetries:  0,
		BackoffBase: time.Millisecond,
	}, nil)

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
			_ = d.Send(a)
		}()
	}
	close(start)

	// Let both goroutines pile up: the first should be blocked in the HTTP
	// handler (via <-release), the second should have already returned no-op.
	time.Sleep(30 * time.Millisecond)
	assert.Equal(t, int32(1), attempts.Load(),
		"only one of the concurrent Send calls should reach the webhook")

	close(release)
	wg.Wait()

	// After the first delivery completes, inFlight is released; a fresh Send
	// for the same key should go through.
	require.NoError(t, d.Send(a))
	assert.Equal(t, int32(2), attempts.Load(),
		"Send after inFlight release must be allowed through")
}

// TestDispatcher_RecordSentOnFailureDedupsOutageStorm locks in the failure-path
// dedup contract: when a webhook receiver returns 5xx, Send records lastSent
// anyway so a repeat page inside the dedup window is suppressed. Without this,
// an outage that keeps returning 5xx would produce webhook spam at every retry
// cycle (defeating dedup exactly when the receiver needs backpressure most).
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

	a := alerts.Alert{Type: "disk_full", Severity: alerts.SeverityWarning, Resource: "node-3", Message: "m"}
	require.Error(t, d.Send(a))
	firstAttempts := attempts.Load()
	require.Equal(t, int32(3), firstAttempts, "1 initial + 2 retries before failure surfaces")

	clock.advance(5 * time.Minute) // still inside dedup window
	require.NoError(t, d.Send(a), "suppressed call returns nil (matches success-path dedup contract)")
	assert.Equal(t, firstAttempts, attempts.Load(),
		"second Send within dedup window must NOT hit the receiver again")

	clock.advance(6 * time.Minute) // 11 min total → outside window
	require.Error(t, d.Send(a))
	assert.Greater(t, attempts.Load(), firstAttempts,
		"after dedup window passes, a fresh failing delivery is permitted")
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
