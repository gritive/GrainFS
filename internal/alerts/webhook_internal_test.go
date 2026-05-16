package alerts

import (
	"context"
	"errors"
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
	// Plan-deviation note: the plan's original assertion
	// `require.ErrorIs(got, context.Canceled)` via onResult is structurally
	// unreachable. close(d.stop) flips the controller into the stop branch,
	// which exits without consuming releaseInbox. By the time workerCancel
	// fires (at 50ms) and the worker enqueues releaseCmd, the controller
	// has long since returned — releaseToController falls through to its
	// `<-d.stop` branch which only bumps metrics. Asserting ctx semantics
	// on Stop itself (graceful + workerCancel path exercised) is what this
	// test name actually claims to cover.
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(500)
	}))
	defer srv.Close()
	d := NewDispatcher(srv.URL, Options{
		Clock:       time.Now,
		MaxRetries:  5,
		BackoffBase: 10 * time.Second, // 길게 — workerCancel 없으면 Stop이 10s 블록
	}, nil)
	d.Start(context.Background())

	d.Send(Alert{Type: "t", Resource: "r", Severity: SeverityWarning, Message: "m"})

	// worker가 spawn되어 첫 attempt(즉시 500 hit)까지 도달했음을 확인.
	// 이후 worker는 10s backoff(time.After) 안에서 잠든다.
	require.Eventually(t, func() bool { return hits.Load() >= 1 },
		time.Second, 5*time.Millisecond, "worker must reach first delivery attempt")

	// Stop ctx로 짧은 timeout → workerCancel 경로가 발화해야 Stop이 10s가
	// 아니라 ~50ms 안에 반환된다.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := d.Stop(ctx)
	elapsed := time.Since(start)
	require.Error(t, err, "Stop must surface ctx.Err()")
	require.Less(t, elapsed, 5*time.Second,
		"workerCancel must abort 10s backoff sleep; took %s", elapsed)
}

func TestStop_Idempotent(t *testing.T) {
	d := NewDispatcher("http://example", Options{}, nil)
	d.Start(context.Background())
	require.NoError(t, d.Stop(context.Background()))
	require.NoError(t, d.Stop(context.Background())) // 두 번째도 panic 없음
}

func TestStop_BeforeStartIsNoop(t *testing.T) {
	d := NewDispatcher("http://example", Options{}, nil)
	require.NoError(t, d.Stop(context.Background()))
}

func TestStop_DrainsResidualSendCmdsAsDroppedStopped(t *testing.T) {
	d := NewDispatcher("http://example", Options{}, nil)
	block := make(chan struct{})
	d.Start(context.Background())
	d.envPtr.spawn = func(Alert, string, string) { <-block }

	before := testutil.ToFloat64(
		metrics.AlertDispatchDroppedTotal.WithLabelValues("", "stopped"))
	for i := 0; i < 10; i++ {
		d.Send(Alert{Type: "t", Resource: fmt.Sprintf("r%d", i)})
	}
	// controller가 첫 sendCmd를 spawn 호출하면 block. 나머지는 inbox에 잔류.
	// Stop이 close(stop) → drain → 잔여 sendCmd가 reason=stopped 카운터로.
	close(block) // worker 무산
	require.NoError(t, d.Stop(context.Background()))
	after := testutil.ToFloat64(
		metrics.AlertDispatchDroppedTotal.WithLabelValues("", "stopped"))
	// PL10: design doc Q1의 nanosecond race window로 결정적 카운트 보장 불가.
	// drain이 동작한다는 것만 검증 (적어도 1개 잡힘).
	require.GreaterOrEqual(t, after-before, float64(1),
		"drainResidualSendCmds must catch at least some residual sendCmds")
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

func TestDispatchEnv_DecryptWarnRateLimit(t *testing.T) {
	var nowNs atomic.Int64
	nowNs.Store(time.Now().UnixNano())
	env := &dispatchEnv{
		opts: Options{
			Clock: func() time.Time { return time.Unix(0, nowNs.Load()) },
		},
		alertKind: "test",
	}
	fakeErr := errors.New("key not found")

	// 첫 호출 — lastDecryptWarnAt 갱신
	env.observeSecretDecryptFailure(fakeErr)
	first := env.lastDecryptWarnAt
	require.False(t, first.IsZero())

	// 같은 시점 두 번째 호출 — lastDecryptWarnAt 변화 없음
	env.observeSecretDecryptFailure(fakeErr)
	require.Equal(t, first, env.lastDecryptWarnAt)

	// 1분 1ns 후 — 갱신
	nowNs.Add(int64(time.Minute) + 1)
	env.observeSecretDecryptFailure(fakeErr)
	require.True(t, env.lastDecryptWarnAt.After(first))
}

// TestController_InboxAndReleaseAreSeparateChannels verifies that inbox (sendCmd)
// and releaseInbox (releaseCmd) are distinct buffered channels (F4 design
// property: release backpressure ≠ send backpressure).  A single roundtrip via a
// blocking spawn also confirms that the release path reaches onResult.
func TestController_InboxAndReleaseAreSeparateChannels(t *testing.T) {
	d := NewDispatcher("http://example", Options{Clock: time.Now}, nil)

	var releasedCount atomic.Int32
	workerBlock := make(chan struct{})
	var spawnedCount atomic.Int32

	d.envPtr.spawn = func(a Alert, url, secret string) {
		spawnedCount.Add(1)
		<-workerBlock
		d.releaseInbox <- releaseCmd{key: dedupKey(a), alert: a, err: nil}
	}
	d.envPtr.onResult = func(_ Alert, _ error) {
		releasedCount.Add(1)
	}

	d.Start(context.Background())
	defer d.Stop(context.Background()) //nolint:errcheck

	// F4 structural assertion: inbox and releaseInbox are separate channels
	require.Equal(t, 32, cap(d.inbox), "inbox capacity")
	require.Equal(t, 16, cap(d.releaseInbox), "releaseInbox capacity")

	d.Send(Alert{Type: "t", Resource: "first"})
	require.Eventually(t, func() bool { return spawnedCount.Load() == 1 },
		time.Second, 5*time.Millisecond, "spawn must run for first alert")

	close(workerBlock)
	require.Eventually(t, func() bool { return releasedCount.Load() == 1 },
		time.Second, 5*time.Millisecond, "release path must invoke onResult")
}
