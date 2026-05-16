package alerts

import (
	"context"
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
