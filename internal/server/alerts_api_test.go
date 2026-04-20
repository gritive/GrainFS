package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	hertz "github.com/cloudwego/hertz/pkg/app/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/alerts"
)

type fakeReceiver struct {
	srv      *httptest.Server
	attempts atomic.Int32
	failNext atomic.Int32
}

func newFailingReceiver(t *testing.T, fails int) *fakeReceiver {
	t.Helper()
	r := &fakeReceiver{}
	r.failNext.Store(int32(fails))
	r.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		r.attempts.Add(1)
		if r.failNext.Load() > 0 {
			r.failNext.Add(-1)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(r.srv.Close)
	return r
}

// startTestAlertsServer spins up a real Hertz server on a free localhost port
// with only the alerts endpoints registered. Returns its base URL and a
// shutdown helper. Real localhost is required because /api/admin/alerts/*
// runs through localhostOnly() — synthetic ut requests fail with 403.
func startTestAlertsServer(t *testing.T, st *AlertsState) string {
	t.Helper()
	port := freeLocalPort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	h := hertz.Default(
		hertz.WithHostPorts(addr),
		hertz.WithExitWaitTime(time.Second),
	)
	srv := &Server{alerts: st}
	srv.registerAlertsAPI(h)

	go h.Spin()

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = h.Shutdown(ctx)
	})

	base := "http://" + addr
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			conn.Close()
			return base
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("test alerts server never came up on %s", addr)
	return ""
}

func freeLocalPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func getJSON(t *testing.T, url string, into any) {
	t.Helper()
	resp, err := http.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "GET %s", url)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(body, into))
}

func postJSON(t *testing.T, url string, into any) int {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(""))
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	if into != nil {
		require.NoError(t, json.Unmarshal(body, into))
	}
	return resp.StatusCode
}

func TestAlertsStatus_HealthyResponseHasNoFailures(t *testing.T) {
	r := newFailingReceiver(t, 0)
	st := NewAlertsState(r.srv.URL, alerts.Options{
		MaxRetries:  0,
		BackoffBase: time.Millisecond,
	}, alerts.DegradedConfig{})
	base := startTestAlertsServer(t, st)

	require.NoError(t, st.Send(alerts.Alert{Type: "ok", Severity: alerts.SeverityWarning, Message: "ok"}))

	var got alertsStatusResponse
	getJSON(t, base+"/api/admin/alerts/status", &got)

	assert.False(t, got.Degraded)
	assert.EqualValues(t, 1, got.DeliveredOK)
	assert.EqualValues(t, 0, got.DeliveryFailed)
	assert.Empty(t, got.LastFailedType, "no failed alert means no banner")
}

func TestAlertsStatus_FailureSurfacesForBanner(t *testing.T) {
	r := newFailingReceiver(t, 999)
	st := NewAlertsState(r.srv.URL, alerts.Options{
		MaxRetries:  1,
		BackoffBase: time.Millisecond,
		BackoffCap:  2 * time.Millisecond,
	}, alerts.DegradedConfig{})
	base := startTestAlertsServer(t, st)

	require.Error(t, st.Send(alerts.Alert{
		Type:     "raft_quorum_lost",
		Severity: alerts.SeverityCritical,
		Resource: "cluster-1",
		Message:  "lost",
	}))

	var got alertsStatusResponse
	getJSON(t, base+"/api/admin/alerts/status", &got)
	assert.EqualValues(t, 0, got.DeliveredOK)
	assert.EqualValues(t, 1, got.DeliveryFailed)
	assert.Equal(t, "raft_quorum_lost", got.LastFailedType, "banner needs the type to render")
	assert.NotEmpty(t, got.LastFailedErr)
}

func TestAlertsResend_ClearsBannerOnSuccess(t *testing.T) {
	// 1 try + 1 retry both fail (2 failures), then resend tries lands on
	// the third (now-healthy) attempt.
	r := newFailingReceiver(t, 2)
	st := NewAlertsState(r.srv.URL, alerts.Options{
		MaxRetries:  1,
		BackoffBase: time.Millisecond,
		BackoffCap:  2 * time.Millisecond,
	}, alerts.DegradedConfig{})
	base := startTestAlertsServer(t, st)

	require.Error(t, st.Send(alerts.Alert{Type: "x", Severity: alerts.SeverityWarning, Message: "m"}))

	var resend struct {
		Resent bool   `json:"resent"`
		Error  string `json:"error"`
	}
	require.Equal(t, http.StatusOK, postJSON(t, base+"/api/admin/alerts/resend", &resend))
	require.True(t, resend.Resent, "resend must succeed once receiver heals: %s", resend.Error)

	var got alertsStatusResponse
	getJSON(t, base+"/api/admin/alerts/status", &got)
	assert.Empty(t, got.LastFailedType, "successful resend must clear the banner")
}

func TestAlertsResend_NoFailedAlertReturnsFalse(t *testing.T) {
	st := NewAlertsState("", alerts.Options{}, alerts.DegradedConfig{})
	base := startTestAlertsServer(t, st)

	var out map[string]any
	require.Equal(t, http.StatusOK, postJSON(t, base+"/api/admin/alerts/resend", &out))
	assert.Equal(t, false, out["resent"])
	assert.Contains(t, out["reason"], "no failed alert")
}

func TestGaugeTracker_MirrorsDegradedState(t *testing.T) {
	// Pass ExitStableWindow=-1 to suppress the "all-zero = production
	// defaults" fallback so we get exit-on-tick semantics.
	st := NewAlertsState("", alerts.Options{}, alerts.DegradedConfig{
		ExitStableWindow: -1,
	})
	g := st.Tracker()

	g.Report(true, "shard_unrepairable", "b/k")
	assert.True(t, g.Degraded())

	g.Report(false, "", "")
	assert.True(t, g.Degraded(), "negative window falls back to default 30s, must remain degraded")

	// Use a fresh state with explicitly-zero (immediate exit) window via
	// a non-empty FlapWindow to bypass the zero-config fallback.
	st2 := NewAlertsState("", alerts.Options{}, alerts.DegradedConfig{
		ExitStableWindow: 0,
		FlapWindow:       1 * time.Second,
		FlapThreshold:    99,
	})
	g2 := st2.Tracker()
	g2.Report(true, "x", "y")
	assert.True(t, g2.Degraded())
	g2.Report(false, "", "")
	assert.False(t, g2.Degraded(), "zero ExitStableWindow + non-default config = exit on next healthy report")
}
