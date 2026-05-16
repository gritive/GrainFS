package alerts

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/metrics"
)

func TestObserveDrop_WarnRateLimitedToOnePerMinute(t *testing.T) {
	var nowNs atomic.Int64
	nowNs.Store(time.Now().UnixNano())
	d := newDispatcher(Options{
		Clock: func() time.Time { return time.Unix(0, nowNs.Load()) },
	}, nil)

	d.observeDrop(dropReasonInboxFull)
	first := d.lastDropWarnAt.Load()
	require.NotZero(t, first)

	// 같은 시점에 두 번째 호출 → lastDropWarnAt 변경 없음
	d.observeDrop(dropReasonInboxFull)
	require.Equal(t, first, d.lastDropWarnAt.Load())

	// 1분 1ns 후 호출 → 갱신
	nowNs.Add(int64(time.Minute) + 1)
	d.observeDrop(dropReasonInboxFull)
	require.Greater(t, d.lastDropWarnAt.Load(), first)
}

func TestObserveDrop_CounterDeltaCapture(t *testing.T) {
	d := newDispatcher(Options{Clock: time.Now}, nil)
	d.envPtr.alertKind = "test"
	before := testutil.ToFloat64(
		metrics.AlertDispatchDroppedTotal.WithLabelValues("test", "inbox_full"))
	d.observeDrop(dropReasonInboxFull)
	d.observeDrop(dropReasonInboxFull)
	after := testutil.ToFloat64(
		metrics.AlertDispatchDroppedTotal.WithLabelValues("test", "inbox_full"))
	require.Equal(t, float64(2), after-before)
}

func TestSend_NotStartedDropsWithReason(t *testing.T) {
	d := NewDispatcher("http://example", Options{Clock: time.Now}, nil)
	// Start 호출 안 함
	before := testutil.ToFloat64(
		metrics.AlertDispatchDroppedTotal.WithLabelValues("", "not_started"))
	d.Send(Alert{Type: "t"})
	after := testutil.ToFloat64(
		metrics.AlertDispatchDroppedTotal.WithLabelValues("", "not_started"))
	require.Equal(t, float64(1), after-before)
}

func TestSend_StoppingDropsWithReason(t *testing.T) {
	d := NewDispatcher("http://example", Options{Clock: time.Now}, nil)
	d.Start(context.Background())
	require.NoError(t, d.Stop(context.Background()))
	before := testutil.ToFloat64(
		metrics.AlertDispatchDroppedTotal.WithLabelValues("", "stopped"))
	d.Send(Alert{Type: "t"}) // post-Stop
	after := testutil.ToFloat64(
		metrics.AlertDispatchDroppedTotal.WithLabelValues("", "stopped"))
	require.Equal(t, float64(1), after-before)
}

func TestSend_InboxFullDropsWithReason(t *testing.T) {
	d := NewDispatcher("http://example", Options{Clock: time.Now}, nil)
	d.Start(context.Background())
	defer d.Stop(context.Background()) //nolint:errcheck
	block := make(chan struct{})
	d.envPtr.spawn = func(Alert, string, string) { <-block }
	before := testutil.ToFloat64(
		metrics.AlertDispatchDroppedTotal.WithLabelValues("", "inbox_full"))
	for i := 0; i < 64; i++ { // inbox 32 + outstanding 1 + 여유
		d.Send(Alert{Type: "t", Resource: fmt.Sprintf("r%d", i)})
	}
	after := testutil.ToFloat64(
		metrics.AlertDispatchDroppedTotal.WithLabelValues("", "inbox_full"))
	require.GreaterOrEqual(t, after-before, float64(1))
	close(block)
}

func TestRetryAndDeliver_SuccessSendsRelease(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	defer srv.Close()
	d := NewDispatcher(srv.URL, Options{Clock: time.Now}, nil)
	d.Start(context.Background())
	defer d.Stop(context.Background()) //nolint:errcheck

	done := make(chan struct{})
	var gotAlert Alert
	var gotErr error
	d.envPtr.onResult = func(a Alert, err error) {
		gotAlert = a
		gotErr = err
		close(done)
	}

	d.Send(Alert{Type: "t", Resource: "r", Severity: SeverityWarning, Message: "m"})
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("onResult not invoked in time")
	}
	require.Equal(t, "t", gotAlert.Type)
	require.NoError(t, gotErr)
}

func TestRetryAndDeliver_CtxCancelStopsBackoff(t *testing.T) {
	t.Skip("requires Task 9: Stop must honor ctx deadline and invoke workerCancel before drain")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(500)
	}))
	defer srv.Close()
	d := NewDispatcher(srv.URL, Options{
		Clock:       time.Now,
		MaxRetries:  5,
		BackoffBase: 10 * time.Second,
	}, nil)
	d.Start(context.Background())

	done := make(chan error, 1)
	d.envPtr.onResult = func(_ Alert, err error) {
		select {
		case done <- err:
		default:
		}
	}
	d.Send(Alert{Type: "t", Resource: "r", Severity: SeverityWarning, Message: "m"})

	// 즉시 Stop ctx로 짧은 timeout → workerCancel
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := d.Stop(ctx)
	require.Error(t, err) // ctx deadline exceeded
	select {
	case got := <-done:
		require.Error(t, got, "worker must surface error via onResult")
	case <-time.After(2 * time.Second):
		t.Fatal("onResult not invoked")
	}
}

func TestController_ProcessesSendThenRelease(t *testing.T) {
	d := newDispatcher(Options{Clock: time.Now, DedupWindow: time.Minute}, nil)
	d.envPtr.url = "http://example"

	done := make(chan Alert, 1)
	d.envPtr.onResult = func(a Alert, _ error) { done <- a }
	d.envPtr.spawnTestHook = func(a Alert, url, secret string) {
		// ephemeral worker 흉내 — release 보냄
		d.releaseInbox <- releaseCmd{key: dedupKey(a), alert: a, err: nil}
	}

	d.Start(context.Background())
	defer d.Stop(context.Background()) //nolint:errcheck

	d.inbox <- sendCmd{alert: Alert{Type: "t", Resource: "r"}}

	select {
	case got := <-done:
		require.Equal(t, "t", got.Type)
	case <-time.After(time.Second):
		t.Fatal("controller did not deliver release")
	}
}
