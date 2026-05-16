package alerts

import (
	"context"
	"fmt"
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
