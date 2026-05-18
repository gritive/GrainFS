package cluster

import (
	"context"
	"sync"
)

// coalesceJob identifies one appendable object to evaluate / coalesce.
type coalesceJob struct {
	Bucket string
	Key    string
}

type coalesceProcessFn func(ctx context.Context, job coalesceJob) error

// coalesceWorker is a single-goroutine queue that dedups jobs by
// (Bucket, Key) and never blocks the producer. If the channel buffer is
// full, the enqueue is dropped — the periodic backstop scanner (Task 10)
// recovers any miss.
type coalesceWorker struct {
	queue chan coalesceJob
	fn    coalesceProcessFn

	mu       sync.Mutex
	inflight map[coalesceJob]bool
	pending  map[coalesceJob]bool

	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once
}

func newCoalesceWorker(bufferSize int, fn coalesceProcessFn) *coalesceWorker {
	return &coalesceWorker{
		queue:    make(chan coalesceJob, bufferSize),
		fn:       fn,
		inflight: map[coalesceJob]bool{},
		pending:  map[coalesceJob]bool{},
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
}

// Start launches the worker goroutine. Safe to call once; calling twice
// panics on the close(w.done).
func (w *coalesceWorker) Start(ctx context.Context) {
	go w.loop(ctx)
}

// Stop signals the worker to exit and waits for it. Idempotent: subsequent
// calls observe the already-closed stop channel and return after the worker
// has drained.
func (w *coalesceWorker) Stop() {
	w.stopOnce.Do(func() {
		close(w.stop)
	})
	<-w.done
}

// Enqueue is non-blocking. If (bucket, key) is already queued or in-flight,
// the enqueue is collapsed (dedup). If the channel is full, the job is
// dropped — periodic backstop scanner (Task 10) recovers any miss.
func (w *coalesceWorker) Enqueue(j coalesceJob) {
	w.mu.Lock()
	if w.inflight[j] || w.pending[j] {
		w.mu.Unlock()
		return
	}
	w.pending[j] = true
	w.mu.Unlock()
	select {
	case w.queue <- j:
	default:
		w.mu.Lock()
		delete(w.pending, j)
		w.mu.Unlock()
	}
}

func (w *coalesceWorker) loop(ctx context.Context) {
	defer close(w.done)
	for {
		select {
		case <-w.stop:
			return
		case <-ctx.Done():
			return
		case j := <-w.queue:
			w.mu.Lock()
			delete(w.pending, j)
			w.inflight[j] = true
			w.mu.Unlock()
			_ = w.fn(ctx, j) // errors handled by fn (logged + metrics)
			w.mu.Lock()
			delete(w.inflight, j)
			w.mu.Unlock()
		}
	}
}
