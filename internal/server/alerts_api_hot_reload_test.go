package server

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/alerts"
	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/encrypt"
)

// recordingReceiver captures every webhook delivery for later assertion.
type recordingReceiver struct {
	mu       sync.Mutex
	requests []recordedRequest
	srv      *httptest.Server
}

type recordedRequest struct {
	body      []byte
	signature string
}

func newRecordingReceiver(t *testing.T) *recordingReceiver {
	rr := &recordingReceiver{}
	rr.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		rr.mu.Lock()
		rr.requests = append(rr.requests, recordedRequest{
			body:      body,
			signature: r.Header.Get("X-GrainFS-Signature"),
		})
		rr.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(rr.srv.Close)
	return rr
}

func (rr *recordingReceiver) waitForRequest(t *testing.T, n int) recordedRequest {
	t.Helper()
	var got recordedRequest
	require.Eventually(t, func() bool {
		rr.mu.Lock()
		defer rr.mu.Unlock()
		if len(rr.requests) >= n {
			got = rr.requests[n-1]
			return true
		}
		return false
	}, 5*time.Second, 25*time.Millisecond, "receiver did not get request #%d within 5s", n)
	return got
}

func (rr *recordingReceiver) count() int {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	return len(rr.requests)
}

func expectedSignature(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

// TestAlertsState_HotReload_WebhookURLAndSecret verifies that a
// cluster_config PATCH that rotates AlertWebhook AND
// AlertWebhookSecretWrapped reaches the NEXT delivery without a process
// restart. This is the deferred Slice 1 verification flagged in the v1.1
// hardening spec.
//
// Integration scope (intentional substitute for the planned e2e form): runs
// the full production code path EXCEPT the admin UDS PATCH (admin UDS
// delivery is covered separately by the audit integration test in Batch 5).
// Drives state.Send directly so no degraded-mode induction is needed.
//
// Distinct Resource values ("r1" vs "r2") keep the dedup key (Type|Resource)
// different between the two sends, so the default DedupWindow does not
// suppress the second delivery.
func TestAlertsState_HotReload_WebhookURLAndSecret(t *testing.T) {
	key := bytes.Repeat([]byte{0x42}, 32)
	enc, err := encrypt.NewEncryptor(key)
	require.NoError(t, err)

	fsm := cluster.NewMetaFSM()
	fsm.SetEncryptor(enc) // required before any patch carrying AlertWebhookSecretWrapped

	state := NewAlertsStateWithConfig(
		fsm.ClusterConfig(),
		enc,
		cluster.ClusterConfigAlertSecretAAD,
		alerts.Options{DedupWindow: 0, MaxRetries: 0},
		alerts.DegradedConfig{},
		"hot-reload-test",
	)
	// Start the dispatcher actor (production wires Start in a later task; the
	// hot-reload contract exercised here lives wholly inside the actor pattern).
	state.dispatcher.Start(context.Background())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = state.dispatcher.Stop(ctx)
	})
	t.Cleanup(state.Close)

	rcv1 := newRecordingReceiver(t)
	rcv2 := newRecordingReceiver(t)

	// Phase 1: wire webhook to rcv1 + secret-v1.
	secret1 := "secret-v1"
	wrapped1, err := enc.EncryptWithAAD([]byte(secret1), cluster.ClusterConfigAlertSecretAAD)
	require.NoError(t, err)
	// ClusterConfig validator requires https:// or http://localhost; httptest
	// emits http://127.0.0.1:PORT, so rewrite the host to satisfy the gate.
	url1 := strings.Replace(rcv1.srv.URL, "127.0.0.1", "localhost", 1)
	require.NoError(t, fsm.ApplyClusterConfigPatchForTest(cluster.ClusterConfigPatch{
		AlertWebhook:              &url1,
		AlertWebhookSecretWrapped: wrapped1,
	}))

	a1 := alerts.Alert{Type: "test", Severity: alerts.SeverityWarning, Resource: "r1", Message: "first alert", Time: time.Now()}
	state.Send(a1)
	got1 := rcv1.waitForRequest(t, 1)
	require.Equal(t, expectedSignature(secret1, got1.body), got1.signature,
		"alert 1 must be signed with secret-v1")

	// Phase 2: rotate to rcv2 + secret-v2 — same FSM, different patch.
	secret2 := "secret-v2-rotated"
	wrapped2, err := enc.EncryptWithAAD([]byte(secret2), cluster.ClusterConfigAlertSecretAAD)
	require.NoError(t, err)
	url2 := strings.Replace(rcv2.srv.URL, "127.0.0.1", "localhost", 1)
	require.NoError(t, fsm.ApplyClusterConfigPatchForTest(cluster.ClusterConfigPatch{
		AlertWebhook:              &url2,
		AlertWebhookSecretWrapped: wrapped2,
	}))

	a2 := alerts.Alert{Type: "test", Severity: alerts.SeverityWarning, Resource: "r2", Message: "second alert", Time: time.Now()}
	state.Send(a2)
	got2 := rcv2.waitForRequest(t, 1)
	require.Equal(t, expectedSignature(secret2, got2.body), got2.signature,
		"alert 2 must be signed with secret-v2 (rotated)")

	// Phase 3: rcv1 must NOT have received the second alert.
	require.Equal(t, 1, rcv1.count(), "rcv1 should only have alert 1 — rotation should redirect")
}
