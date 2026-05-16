package alerts

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/metrics"
)

// startDispatcherInternal is the internal-package version of the helper used in
// webhook_test.go: starts d and arranges Stop on cleanup so the controller
// goroutine cannot leak into subsequent tests.
func startDispatcherInternal(t *testing.T, d *Dispatcher) {
	t.Helper()
	d.Start(context.Background())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = d.Stop(ctx)
	})
}

// stubDecrypter always returns the configured error.
type stubDecrypter struct{ err error }

func (s *stubDecrypter) DecryptWithAAD(_, _ []byte) ([]byte, error) { return nil, s.err }

// stubCfg returns a fixed URL + a fixed wrapped-secret.
type stubCfg struct {
	url     string
	wrapped []byte
}

func (s *stubCfg) AlertWebhook() string              { return s.url }
func (s *stubCfg) AlertWebhookSecretWrapped() []byte { return s.wrapped }

func TestWebhook_DecryptFailure_EmitsMetricAndLogUnsigned(t *testing.T) {
	metrics.WebhookSignatureDecryptFailureTotal.Reset()

	var logged bytes.Buffer
	prevLogger := log.Logger
	log.Logger = zerolog.New(&logged).With().Timestamp().Logger()
	t.Cleanup(func() { log.Logger = prevLogger })

	var receivedSig atomic.Value // string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedSig.Store(r.Header.Get("X-GrainFS-Signature"))
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	cfg := &stubCfg{url: srv.URL, wrapped: []byte("ciphertext")}
	dec := &stubDecrypter{err: errors.New("key not found")}

	now := time.Date(2026, 5, 12, 12, 0, 0, 0, time.UTC)
	d := NewDispatcherWithConfig(cfg, dec, []byte("aad"),
		Options{Clock: func() time.Time { return now }, MaxRetries: 0, DedupWindow: 0},
		nil, "degraded")
	startDispatcherInternal(t, d)

	d.Send(Alert{Type: "t", Severity: SeverityWarning, Resource: "r", Message: "m", Time: now})
	d.DrainForTest()

	require.InDelta(t, 1.0,
		testutil.ToFloat64(metrics.WebhookSignatureDecryptFailureTotal.WithLabelValues("degraded", "key_not_found")),
		0.0001)

	got, _ := receivedSig.Load().(string)
	require.Empty(t, got, "expected unsigned delivery, got X-GrainFS-Signature=%q", got)

	require.Equal(t, 1, strings.Count(logged.String(), `"event":"webhook_signature_decrypt_failure"`),
		"expected exactly 1 warn log line; got log=%q", logged.String())
}

func TestWebhook_DecryptFailure_LogRateLimited_MetricNotRateLimited(t *testing.T) {
	metrics.WebhookSignatureDecryptFailureTotal.Reset()

	var logged bytes.Buffer
	prevLogger := log.Logger
	log.Logger = zerolog.New(&logged).With().Timestamp().Logger()
	t.Cleanup(func() { log.Logger = prevLogger })

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	cfg := &stubCfg{url: srv.URL, wrapped: []byte("ciphertext")}
	dec := &stubDecrypter{err: errors.New("aad mismatch")}

	now := time.Date(2026, 5, 12, 12, 0, 0, 0, time.UTC)
	d := NewDispatcherWithConfig(cfg, dec, []byte("aad"),
		Options{Clock: func() time.Time { return now }, MaxRetries: 0, DedupWindow: 0},
		nil, "degraded")
	startDispatcherInternal(t, d)

	// Send 100 alerts with distinct Resource keys (DedupWindow=0 also disables dedup).
	// Drain between batches so the 32-slot inbox cannot drop alerts as
	// dropReasonInboxFull — this test asserts a metric increment of exactly 100.
	for i := 0; i < 100; i++ {
		res := "r" + string(rune('a'+(i%26))) + string(rune('a'+((i/26)%26)))
		d.Send(Alert{Type: "t", Severity: SeverityWarning, Resource: res, Message: "m", Time: now})
		if i%16 == 15 {
			d.DrainForTest()
		}
	}
	d.DrainForTest()

	require.InDelta(t, 100.0,
		testutil.ToFloat64(metrics.WebhookSignatureDecryptFailureTotal.WithLabelValues("degraded", "aad_mismatch")),
		0.0001)
	require.Equal(t, 1, strings.Count(logged.String(), `"event":"webhook_signature_decrypt_failure"`),
		"expected exactly 1 log line under rate limit; got log=%q", logged.String())
}

// TestWebhook_DecryptFailure_RealEncryptorClassification verifies the
// production path: a real *encrypt.Encryptor configured with one key
// receives a blob wrapped under a DIFFERENT key. AEAD tag verification
// fails ("decrypt: cipher: message authentication failed"), which
// classifyDecryptErr should bucket as "aad_mismatch" (not "other").
//
// This test exists because the original substrings in classifyDecryptErr
// did not match real encrypt errors (caught by Batch 7 code review). The
// matcher was broadened in this commit to recognize "message authentication
// failed" — this test is the regression guard.
func TestWebhook_DecryptFailure_RealEncryptorClassification(t *testing.T) {
	metrics.WebhookSignatureDecryptFailureTotal.Reset()

	prevLogger := log.Logger
	log.Logger = zerolog.New(io.Discard).With().Logger()
	t.Cleanup(func() { log.Logger = prevLogger })

	keyA := bytes.Repeat([]byte{0xab}, 32)
	keyB := bytes.Repeat([]byte{0xcd}, 32)
	encA, err := encrypt.NewEncryptor(keyA)
	require.NoError(t, err)
	encB, err := encrypt.NewEncryptor(keyB)
	require.NoError(t, err)

	aad := []byte("v1.1-hardening-test")
	wrappedUnderA, err := encA.EncryptWithAAD([]byte("secret"), aad)
	require.NoError(t, err)

	// Sanity-check: the real DecryptWithAAD on encB with a blob wrapped under
	// encA fails. Capture the error so we can assert classification.
	_, derr := encB.DecryptWithAAD(wrappedUnderA, aad)
	require.Error(t, derr, "expected decrypt under a different key to fail")
	require.Equal(t, "aad_mismatch", classifyDecryptErr(derr),
		"AEAD tag failure should classify as aad_mismatch, got %q (err=%v)", classifyDecryptErr(derr), derr)

	// Drive resolveLive end-to-end with the wrong-key encryptor.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	cfg := &stubCfg{url: srv.URL, wrapped: wrappedUnderA}
	now := time.Date(2026, 5, 12, 12, 0, 0, 0, time.UTC)
	d := NewDispatcherWithConfig(cfg, encB, aad,
		Options{Clock: func() time.Time { return now }, MaxRetries: 0, DedupWindow: 0},
		nil, "cluster")
	startDispatcherInternal(t, d)

	d.Send(Alert{Type: "t", Severity: SeverityWarning, Resource: "r", Message: "m", Time: now})
	d.DrainForTest()

	require.InDelta(t, 1.0,
		testutil.ToFloat64(metrics.WebhookSignatureDecryptFailureTotal.WithLabelValues("cluster", "aad_mismatch")),
		0.0001)
}

// TestClassifyDecryptErr_BoundedEnum confirms the matcher returns ONE of the
// five labels we use as metric values. Cardinality protection.
func TestClassifyDecryptErr_BoundedEnum(t *testing.T) {
	allowed := map[string]bool{
		"none": true, "key_not_found": true, "aad_mismatch": true,
		"decode_error": true, "other": true,
	}
	cases := []error{
		nil,
		errors.New("key not found"),
		errors.New("unknown key id 42"),
		errors.New("aad mismatch"),
		errors.New("AAD verification failed"),
		errors.New("decrypt: cipher: message authentication failed"),
		errors.New("decode: bad base64"),
		errors.New("unmarshal: unexpected eof"),
		errors.New("not an encrypted blob (missing magic header)"),
		errors.New("ciphertext too short"),
		errors.New("genuinely novel failure mode 9000"),
	}
	for _, e := range cases {
		got := classifyDecryptErr(e)
		require.True(t, allowed[got], "classifyDecryptErr(%v) = %q, not in {%v}", e, got, allowed)
	}
}
