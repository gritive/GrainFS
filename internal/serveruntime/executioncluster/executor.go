package executioncluster

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gritive/GrainFS/internal/server/execution"
)

const (
	defaultMailboxCapacity = 64
	defaultJobTimeout      = 30 * time.Second
	defaultMaxAttempts     = 1
	defaultRetryBackoff    = 10 * time.Millisecond
)

type ScrubBackend interface {
	TriggerScrub(context.Context, execution.Operation) (execution.ScrubResult, error)
}

type Executor struct {
	backend      ScrubBackend
	mailbox      chan *job
	metrics      Metrics
	jobTimeout   time.Duration
	maxAttempts  int
	retryBackoff time.Duration

	done   chan struct{}
	closed atomic.Bool
	mu     sync.Mutex
	wg     sync.WaitGroup
	wait   chan struct{}

	closeCtx    context.Context
	closeCancel context.CancelFunc
}

type job struct {
	ctx    context.Context
	op     execution.Operation
	result chan jobResult
}

type jobResult struct {
	result execution.Result
	err    error
}

type Option func(*Executor)

func NewExecutor(backend ScrubBackend, options ...Option) *Executor {
	closeCtx, closeCancel := context.WithCancel(context.Background())
	executor := &Executor{
		backend:      backend,
		mailbox:      make(chan *job, defaultMailboxCapacity),
		metrics:      NoopMetrics{},
		jobTimeout:   defaultJobTimeout,
		maxAttempts:  defaultMaxAttempts,
		retryBackoff: defaultRetryBackoff,
		done:         make(chan struct{}),
		wait:         make(chan struct{}),
		closeCtx:     closeCtx,
		closeCancel:  closeCancel,
	}
	for _, option := range options {
		option(executor)
	}
	if executor.metrics == nil {
		executor.metrics = NoopMetrics{}
	}
	if cap(executor.mailbox) == 0 {
		executor.mailbox = make(chan *job, defaultMailboxCapacity)
	}
	if executor.maxAttempts < 1 {
		executor.maxAttempts = defaultMaxAttempts
	}
	if executor.retryBackoff <= 0 {
		executor.retryBackoff = defaultRetryBackoff
	}

	executor.wg.Add(1)
	go executor.worker()
	go func() {
		executor.wg.Wait()
		close(executor.wait)
	}()

	return executor
}

func WithMetrics(metrics Metrics) Option {
	return func(executor *Executor) {
		executor.metrics = metrics
	}
}

func WithMailboxCapacity(capacity int) Option {
	return func(executor *Executor) {
		if capacity > 0 {
			executor.mailbox = make(chan *job, capacity)
		}
	}
}

func WithJobTimeout(timeout time.Duration) Option {
	return func(executor *Executor) {
		if timeout > 0 {
			executor.jobTimeout = timeout
		}
	}
}

func WithMaxAttempts(maxAttempts int) Option {
	return func(executor *Executor) {
		if maxAttempts > 0 {
			executor.maxAttempts = maxAttempts
		}
	}
}

func WithRetryBackoff(backoff time.Duration) Option {
	return func(executor *Executor) {
		if backoff > 0 {
			executor.retryBackoff = backoff
		}
	}
}

func (e *Executor) Execute(ctx context.Context, plan execution.Plan) (execution.Result, error) {
	if plan.Strategy != execution.StrategyCluster {
		return execution.Result{}, execution.NewError(execution.CodeUnsupported, execution.ErrExecutionUnsupported)
	}
	if err := plan.Operation.Validate(); err != nil {
		return execution.Result{}, err
	}
	if plan.Operation.Kind != execution.OperationScrub || e.backend == nil {
		return execution.Result{}, execution.NewError(execution.CodeUnsupported, execution.ErrExecutionUnsupported)
	}
	op := plan.Operation
	if op.ID == "" {
		id, err := execution.NewRequestID()
		if err != nil {
			return execution.Result{}, execution.NewError(execution.CodeJobFailed, err)
		}
		op.ID = id
	}
	if err := ctx.Err(); err != nil {
		return execution.Result{}, execution.NewError(execution.CodeJobCancelled, execution.ErrJobCancelled)
	}
	if e.closed.Load() {
		return execution.Result{}, execution.NewError(execution.CodeUnsupported, execution.ErrExecutionUnsupported)
	}

	j := &job{
		ctx:    ctx,
		op:     op,
		result: make(chan jobResult, 1),
	}

	e.mu.Lock()
	if e.closed.Load() {
		e.mu.Unlock()
		return execution.Result{}, execution.NewError(execution.CodeUnsupported, execution.ErrExecutionUnsupported)
	}
	select {
	case e.mailbox <- j:
		e.recordQueueDepth()
		e.mu.Unlock()
	case <-ctx.Done():
		e.mu.Unlock()
		return execution.Result{}, execution.NewError(execution.CodeJobCancelled, execution.ErrJobCancelled)
	default:
		e.mu.Unlock()
		return execution.Result{}, execution.NewError(execution.CodeRetry, execution.ErrAdmissionRejected)
	}

	select {
	case result := <-j.result:
		return result.result, result.err
	case <-ctx.Done():
		return execution.Result{}, execution.NewError(execution.CodeJobCancelled, execution.ErrJobCancelled)
	case <-e.done:
		return execution.Result{}, execution.NewError(execution.CodeJobCancelled, execution.ErrJobCancelled)
	}
}

func (e *Executor) Close(ctx context.Context) error {
	e.mu.Lock()
	if e.closed.CompareAndSwap(false, true) {
		e.closeCancel()
		close(e.done)
		e.drainMailbox()
	}
	e.mu.Unlock()

	select {
	case <-e.wait:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *Executor) worker() {
	defer e.wg.Done()

	for {
		select {
		case j := <-e.mailbox:
			e.recordQueueDepth()
			if e.closed.Load() || j.ctx.Err() != nil {
				j.result <- jobResult{err: execution.NewError(execution.CodeJobCancelled, execution.ErrJobCancelled)}
				continue
			}
			e.run(j)
		case <-e.done:
			e.drainMailbox()
			return
		}
	}
}

func (e *Executor) run(j *job) {
	start := time.Now()
	result, err := e.executeScrub(j.ctx, j.op)
	e.metrics.RecordJobDuration(time.Since(start))
	j.result <- jobResult{result: result, err: err}
}

func (e *Executor) executeScrub(parent context.Context, op execution.Operation) (execution.Result, error) {
	baseCtx, baseCancel := mergeContexts(parent, e.closeCtx)
	defer baseCancel()

	ctx := baseCtx
	if e.jobTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(baseCtx, e.jobTimeout)
		defer cancel()
	}

	var lastErr error
	for attempt := 1; attempt <= e.maxAttempts; attempt++ {
		scrubResult, err := e.backend.TriggerScrub(ctx, op)
		if err == nil {
			return execution.Result{Scrub: scrubResult}, nil
		}
		if mapped := e.mapContextError(ctx, err); mapped != nil {
			return execution.Result{}, mapped
		}
		lastErr = err
		if attempt == e.maxAttempts {
			break
		}

		e.metrics.RecordRetry()
		if err := sleepContext(ctx, e.retryBackoff); err != nil {
			if mapped := e.mapContextError(ctx, err); mapped != nil {
				return execution.Result{}, mapped
			}
			lastErr = err
			break
		}
	}

	e.metrics.RecordWorkerFailure()
	return execution.Result{}, execution.NewError(execution.CodeJobFailed, lastErr)
}

func (e *Executor) mapContextError(ctx context.Context, err error) error {
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.Is(ctx.Err(), context.DeadlineExceeded):
		e.metrics.RecordTimeout()
		return execution.NewError(execution.CodeJobTimeout, execution.ErrJobTimedOut)
	case errors.Is(err, context.Canceled), errors.Is(ctx.Err(), context.Canceled):
		return execution.NewError(execution.CodeJobCancelled, execution.ErrJobCancelled)
	default:
		return nil
	}
}

func (e *Executor) drainMailbox() {
	for {
		select {
		case j := <-e.mailbox:
			j.result <- jobResult{err: execution.NewError(execution.CodeJobCancelled, execution.ErrJobCancelled)}
			e.recordQueueDepth()
		default:
			e.recordQueueDepth()
			return
		}
	}
}

func (e *Executor) recordQueueDepth() {
	e.metrics.RecordQueueDepth(len(e.mailbox))
}

func sleepContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func mergeContexts(parent context.Context, closeCtx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	go func() {
		select {
		case <-closeCtx.Done():
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}
