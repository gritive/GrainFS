package executioncluster

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/gritive/GrainFS/internal/server/execution"
)

var errTransientScrub = errors.New("transient scrub failure")

type fakeScrubBackend struct {
	calls   atomic.Int32
	started chan struct{}
	release chan struct{}
	closed  atomic.Bool
	delay   time.Duration
	ids     chan string
	results []execution.ScrubResult
	errs    []error
}

func newFakeScrubBackend() *fakeScrubBackend {
	return &fakeScrubBackend{started: make(chan struct{}, 16), ids: make(chan string, 16)}
}

func (f *fakeScrubBackend) TriggerScrub(ctx context.Context, op execution.Operation) (execution.ScrubResult, error) {
	if op.ID == "" {
		return execution.ScrubResult{}, execution.NewError(execution.CodeInvalid, execution.ErrInvalidOperation)
	}
	call := int(f.calls.Add(1))
	select {
	case f.ids <- op.ID:
	default:
	}
	select {
	case f.started <- struct{}{}:
	default:
	}

	if f.release != nil {
		select {
		case <-f.release:
		case <-ctx.Done():
			return execution.ScrubResult{}, ctx.Err()
		}
	}
	if f.delay > 0 {
		timer := time.NewTimer(f.delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return execution.ScrubResult{}, ctx.Err()
		}
	}

	if call <= len(f.errs) && f.errs[call-1] != nil {
		return execution.ScrubResult{}, f.errs[call-1]
	}
	if call <= len(f.results) {
		return f.results[call-1], nil
	}
	return execution.ScrubResult{SessionID: "scrub-session", Created: true}, nil
}

func (f *fakeScrubBackend) closeRelease() {
	if f.release != nil && f.closed.CompareAndSwap(false, true) {
		close(f.release)
	}
}

func (f *fakeScrubBackend) requireStarted(t *testing.T) {
	t.Helper()
	require.Eventually(t, func() bool {
		return f.calls.Load() > 0
	}, time.Second, time.Millisecond)
}

type fakeMetrics struct {
	QueueDepth chan int
	lastDepth  atomic.Int32
}

func newFakeMetrics() *fakeMetrics {
	metrics := &fakeMetrics{QueueDepth: make(chan int, 16)}
	metrics.lastDepth.Store(-1)
	return metrics
}

func (f *fakeMetrics) RecordQueueDepth(depth int) {
	f.lastDepth.Store(int32(depth))
	select {
	case f.QueueDepth <- depth:
	default:
	}
}

func (f *fakeMetrics) RecordJobDuration(time.Duration) {}
func (f *fakeMetrics) RecordRetry()                    {}
func (f *fakeMetrics) RecordTimeout()                  {}
func (f *fakeMetrics) RecordWorkerFailure()            {}
func (f *fakeMetrics) RecordAggregationFailure()       {}

func TestClusterExecutorExecutesAcceptedScrubJob(t *testing.T) {
	backend := newFakeScrubBackend()
	backend.results = []execution.ScrubResult{{SessionID: "sid-accepted", Created: true}}
	metrics := newFakeMetrics()
	exec := NewExecutor(backend, WithMetrics(metrics), WithMailboxCapacity(4))
	t.Cleanup(func() { require.NoError(t, exec.Close(context.Background())) })

	result, err := exec.Execute(context.Background(), validScrubPlan())

	require.NoError(t, err)
	require.Equal(t, execution.ScrubResult{SessionID: "sid-accepted", Created: true}, result.Scrub)
	require.Equal(t, int32(1), backend.calls.Load())
	requireQueueDepthEventually(t, metrics, 0)
}

func TestClusterExecutorDefaultMailboxCapacityMatchesProductionPlan(t *testing.T) {
	exec := NewExecutor(newFakeScrubBackend())
	t.Cleanup(func() { require.NoError(t, exec.Close(context.Background())) })

	require.Equal(t, 64, cap(exec.mailbox))
}

func TestClusterExecutorRejectsInvalidPlanBeforeAdmission(t *testing.T) {
	backend := newFakeScrubBackend()
	metrics := newFakeMetrics()
	exec := NewExecutor(backend, WithMetrics(metrics), WithMailboxCapacity(1))
	t.Cleanup(func() { require.NoError(t, exec.Close(context.Background())) })

	_, err := exec.Execute(context.Background(), execution.Plan{
		Strategy: execution.StrategyCluster,
		Operation: execution.Operation{
			Kind:  execution.OperationScrub,
			Scrub: execution.ScrubOperation{Bucket: ""}, // empty bucket → invalid
		},
	})

	require.ErrorIs(t, err, execution.ErrInvalidOperation)
	require.Equal(t, execution.CodeInvalid, execution.CodeOf(err))
	require.Equal(t, int32(0), backend.calls.Load())
	require.Equal(t, int32(-1), metrics.lastDepth.Load())
}

func TestClusterExecutorRejectsNonClusterStrategyBeforeAdmission(t *testing.T) {
	backend := newFakeScrubBackend()
	metrics := newFakeMetrics()
	exec := NewExecutor(backend, WithMetrics(metrics), WithMailboxCapacity(1))
	t.Cleanup(func() { require.NoError(t, exec.Close(context.Background())) })

	plan := validScrubPlan()
	plan.Strategy = execution.StrategySingle
	_, err := exec.Execute(context.Background(), plan)

	require.ErrorIs(t, err, execution.ErrExecutionUnsupported)
	require.Equal(t, execution.CodeUnsupported, execution.CodeOf(err))
	require.Equal(t, int32(0), backend.calls.Load())
	require.Equal(t, int32(-1), metrics.lastDepth.Load())
}

func TestClusterExecutorRejectsWhenMailboxFull(t *testing.T) {
	backend := newFakeScrubBackend()
	backend.release = make(chan struct{})
	metrics := newFakeMetrics()
	exec := NewExecutor(backend, WithMetrics(metrics), WithMailboxCapacity(1))
	t.Cleanup(func() {
		backend.closeRelease()
		require.NoError(t, exec.Close(context.Background()))
	})

	firstDone := make(chan error, 1)
	go func() {
		_, err := exec.Execute(context.Background(), validScrubPlan())
		firstDone <- err
	}()
	backend.requireStarted(t)
	drainQueueDepthMetrics(metrics)

	secondDone := make(chan error, 1)
	go func() {
		_, err := exec.Execute(context.Background(), validScrubPlan())
		secondDone <- err
	}()
	requireQueueDepthEventually(t, metrics, 1)

	_, err := exec.Execute(context.Background(), validScrubPlan())

	require.ErrorIs(t, err, execution.ErrAdmissionRejected)
	require.Equal(t, execution.CodeRetry, execution.CodeOf(err))
	backend.closeRelease()
	require.NoError(t, receiveErr(t, firstDone))
	require.NoError(t, receiveErr(t, secondDone))
}

func TestClusterExecutorRecordsQueueDepthAfterDrain(t *testing.T) {
	backend := newFakeScrubBackend()
	backend.release = make(chan struct{})
	metrics := newFakeMetrics()
	exec := NewExecutor(backend, WithMetrics(metrics), WithMailboxCapacity(4))
	t.Cleanup(func() {
		backend.closeRelease()
		require.NoError(t, exec.Close(context.Background()))
	})

	firstDone := make(chan error, 1)
	go func() {
		_, err := exec.Execute(context.Background(), validScrubPlan())
		firstDone <- err
	}()
	backend.requireStarted(t)
	drainQueueDepthMetrics(metrics)

	secondDone := make(chan error, 1)
	go func() {
		_, err := exec.Execute(context.Background(), validScrubPlan())
		secondDone <- err
	}()
	requireQueueDepthEventually(t, metrics, 1)
	backend.closeRelease()
	require.NoError(t, receiveErr(t, firstDone))
	require.NoError(t, receiveErr(t, secondDone))
	requireQueueDepthEventually(t, metrics, 0)
}

func TestClusterExecutorMapsTimeout(t *testing.T) {
	backend := newFakeScrubBackend()
	backend.release = make(chan struct{})
	exec := NewExecutor(backend, WithJobTimeout(10*time.Millisecond), WithMailboxCapacity(1))
	t.Cleanup(func() {
		backend.closeRelease()
		require.NoError(t, exec.Close(context.Background()))
	})

	done := make(chan error, 1)
	go func() {
		_, err := exec.Execute(context.Background(), validScrubPlan())
		done <- err
	}()
	backend.requireStarted(t)
	err := receiveErr(t, done)

	require.ErrorIs(t, err, execution.ErrJobTimedOut)
	require.Equal(t, execution.CodeJobTimeout, execution.CodeOf(err))
	require.Equal(t, int32(1), backend.calls.Load())
}

func TestClusterExecutorMapsCallerCancellation(t *testing.T) {
	backend := newFakeScrubBackend()
	backend.release = make(chan struct{})
	exec := NewExecutor(backend, WithJobTimeout(time.Second), WithMailboxCapacity(1))
	t.Cleanup(func() {
		backend.closeRelease()
		require.NoError(t, exec.Close(context.Background()))
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := exec.Execute(ctx, validScrubPlan())
		done <- err
	}()
	backend.requireStarted(t)
	cancel()

	err := receiveErr(t, done)
	require.ErrorIs(t, err, execution.ErrJobCancelled)
	require.Equal(t, execution.CodeJobCancelled, execution.CodeOf(err))
}

func TestClusterExecutorDoesNotRunQueuedJobAfterCallerCancellation(t *testing.T) {
	backend := newFakeScrubBackend()
	backend.release = make(chan struct{})
	metrics := newFakeMetrics()
	exec := NewExecutor(backend, WithMetrics(metrics), WithJobTimeout(time.Second), WithMailboxCapacity(1))
	t.Cleanup(func() {
		backend.closeRelease()
		require.NoError(t, exec.Close(context.Background()))
	})

	firstDone := make(chan error, 1)
	go func() {
		_, err := exec.Execute(context.Background(), validScrubPlan())
		firstDone <- err
	}()
	backend.requireStarted(t)
	drainQueueDepthMetrics(metrics)

	queuedCtx, cancelQueued := context.WithCancel(context.Background())
	secondDone := make(chan error, 1)
	go func() {
		_, err := exec.Execute(queuedCtx, validScrubPlan())
		secondDone <- err
	}()
	requireQueueDepthEventually(t, metrics, 1)
	cancelQueued()

	err := receiveErr(t, secondDone)
	require.ErrorIs(t, err, execution.ErrJobCancelled)
	require.Equal(t, execution.CodeJobCancelled, execution.CodeOf(err))
	backend.closeRelease()
	require.NoError(t, receiveErr(t, firstDone))
	require.Eventually(t, func() bool {
		return backend.calls.Load() == 1
	}, 100*time.Millisecond, time.Millisecond)
}

func TestClusterExecutorCloseCancelsQueuedJobWithoutRunningBackend(t *testing.T) {
	backend := newFakeScrubBackend()
	backend.release = make(chan struct{})
	metrics := newFakeMetrics()
	exec := NewExecutor(backend, WithMetrics(metrics), WithJobTimeout(time.Second), WithMailboxCapacity(1))

	firstDone := make(chan error, 1)
	go func() {
		_, err := exec.Execute(context.Background(), validScrubPlan())
		firstDone <- err
	}()
	backend.requireStarted(t)
	drainQueueDepthMetrics(metrics)

	secondDone := make(chan error, 1)
	go func() {
		_, err := exec.Execute(context.Background(), validScrubPlan())
		secondDone <- err
	}()
	requireQueueDepthEventually(t, metrics, 1)

	require.NoError(t, exec.Close(context.Background()))
	require.ErrorIs(t, receiveErr(t, firstDone), execution.ErrJobCancelled)
	require.ErrorIs(t, receiveErr(t, secondDone), execution.ErrJobCancelled)
	require.Eventually(t, func() bool {
		return backend.calls.Load() == 1
	}, 100*time.Millisecond, time.Millisecond)
}

func TestClusterExecutorRetriesThenSucceeds(t *testing.T) {
	backend := newFakeScrubBackend()
	backend.errs = []error{errTransientScrub}
	backend.results = []execution.ScrubResult{
		{},
		{SessionID: "sid-retry", Created: false},
	}
	exec := NewExecutor(
		backend,
		WithMaxAttempts(2),
		WithRetryBackoff(time.Millisecond),
		WithMailboxCapacity(1),
	)
	t.Cleanup(func() { require.NoError(t, exec.Close(context.Background())) })

	result, err := exec.Execute(context.Background(), validScrubPlan())

	require.NoError(t, err)
	require.Equal(t, execution.ScrubResult{SessionID: "sid-retry", Created: false}, result.Scrub)
	require.Equal(t, int32(2), backend.calls.Load())
	require.Equal(t, receiveID(t, backend.ids), receiveID(t, backend.ids))
}

func TestClusterExecutorReturnsJobFailedAfterRetryExhaustion(t *testing.T) {
	backend := newFakeScrubBackend()
	backend.errs = []error{errTransientScrub, errTransientScrub}
	exec := NewExecutor(
		backend,
		WithMaxAttempts(2),
		WithRetryBackoff(time.Millisecond),
		WithMailboxCapacity(1),
	)
	t.Cleanup(func() { require.NoError(t, exec.Close(context.Background())) })

	_, err := exec.Execute(context.Background(), validScrubPlan())

	require.Error(t, err)
	require.Equal(t, execution.CodeJobFailed, execution.CodeOf(err))
	require.ErrorIs(t, err, errTransientScrub)
	require.Equal(t, int32(2), backend.calls.Load())
	require.Equal(t, receiveID(t, backend.ids), receiveID(t, backend.ids))
}

func TestClusterExecutorShutdownDoesNotLeakGoroutines(t *testing.T) {
	backend := newFakeScrubBackend()
	exec := NewExecutor(backend, WithMailboxCapacity(2))
	_, err := exec.Execute(context.Background(), validScrubPlan())
	require.NoError(t, err)

	require.NoError(t, exec.Close(context.Background()))
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func validScrubPlan() execution.Plan {
	return execution.Plan{
		Strategy: execution.StrategyCluster,
		Operation: execution.Operation{
			Kind: execution.OperationScrub,
			Scrub: execution.ScrubOperation{
				Bucket: "ec-bucket",
			},
		},
	}
}

func requireQueueDepthEventually(t *testing.T, metrics *fakeMetrics, want int) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for queue depth %d, last depth %d", want, metrics.lastDepth.Load())
		case got := <-metrics.QueueDepth:
			if got == want {
				return
			}
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func drainQueueDepthMetrics(metrics *fakeMetrics) {
	for {
		select {
		case <-metrics.QueueDepth:
		default:
			return
		}
	}
}

func receiveErr(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for executor result")
		return nil
	}
}

func receiveID(t *testing.T, ch <-chan string) string {
	t.Helper()
	select {
	case id := <-ch:
		return id
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for operation ID")
		return ""
	}
}
