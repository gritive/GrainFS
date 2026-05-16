package migration

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// Proposer drives migration job state transitions through the meta-Raft FSM.
type Proposer interface {
	ProposeJobStart(ctx context.Context, bucket string) error
	ProposeJobDone(ctx context.Context, bucket string, copied, errors int64) error
	ProposeJobFailed(ctx context.Context, bucket, reason string, errors int64) error
}

// LeadershipSignal reports and subscribes to Raft leader elections.
// Same shape as lifecycle.LeadershipSignal — see internal/lifecycle/service.go.
type LeadershipSignal interface {
	IsLeader() bool
	Subscribe() (events <-chan struct{}, cancel func())
}

// Service is the deep module for bucket migration jobs. It owns job submission,
// leader-only execution, and status reporting. Callers do not reach past this.
//
// See docs/adr/0012-migration-service-lock-free-publication.md for why the
// worker handle is published lock-free instead of through a controller actor.
type Service struct {
	store      *JobStore
	proposer   Proposer
	leadership LeadershipSignal
	src        Source      // may be nil in unit tests
	dst        Destination // may be nil in unit tests
	interval   time.Duration

	// worker is published by Run()'s reconcile loop. SubmitJob loads it
	// lock-free; nil means this node is not the current leader.
	worker atomic.Pointer[Worker]

	// cancelFn and workerWG are only touched by the Run() goroutine.
	cancelFn context.CancelFunc
	workerWG sync.WaitGroup

	logger zerolog.Logger
}

// NewService wires the migration service. src/dst may be nil for tests that do
// not exercise Run. interval is the Worker polling fallback; 0 disables the
// ticker (useful in tests that drive only via Trigger).
func NewService(store *JobStore, prop Proposer, lead LeadershipSignal, src Source, dst Destination, interval time.Duration) *Service {
	return &Service{
		store:      store,
		proposer:   prop,
		leadership: lead,
		src:        src,
		dst:        dst,
		interval:   interval,
		logger:     log.With().Str("component", "migration-service").Logger(),
	}
}

// SubmitJob proposes a new migration job for bucket through the meta-Raft FSM.
// The leader-only Worker picks it up after the FSM apply writes the job record.
func (s *Service) SubmitJob(ctx context.Context, bucket string) error {
	if err := s.proposer.ProposeJobStart(ctx, bucket); err != nil {
		return fmt.Errorf("migration: submit job: %w", err)
	}
	if w := s.worker.Load(); w != nil {
		w.Trigger()
	}
	return nil
}

// Run watches leadership changes until ctx is done, starting/stopping the Worker.
func (s *Service) Run(ctx context.Context) {
	events, cancel := s.leadership.Subscribe()
	defer cancel()
	s.reconcile(ctx)
	for {
		select {
		case <-ctx.Done():
			s.stop()
			return
		case _, ok := <-events:
			if !ok {
				s.stop()
				return
			}
			s.reconcile(ctx)
		}
	}
}

func (s *Service) reconcile(ctx context.Context) {
	isLeader := s.leadership.IsLeader()
	running := s.worker.Load() != nil
	switch {
	case isLeader && !running:
		s.start(ctx)
	case !isLeader && running:
		s.stop()
	}
}

func (s *Service) start(parent context.Context) {
	if s.worker.Load() != nil {
		return
	}
	if s.src == nil || s.dst == nil {
		s.logger.Warn().Msg("migration executor not started: src/dst unset")
		return
	}
	workerCtx, cancel := context.WithCancel(parent)
	w := newWorker(s.store, s.src, s.dst, s.proposer, s.interval)
	s.cancelFn = cancel
	s.worker.Store(w)
	s.workerWG.Add(1)
	go func() {
		defer s.workerWG.Done()
		s.logger.Info().Msg("starting migration executor (now leader)")
		w.Run(workerCtx)
		s.logger.Info().Msg("migration executor stopped")
	}()
}

func (s *Service) stop() {
	if s.worker.Load() == nil {
		return
	}
	cancel := s.cancelFn
	s.cancelFn = nil
	s.worker.Store(nil)
	if cancel != nil {
		cancel()
	}
	s.workerWG.Wait()
}

// workerRunningForTest exposes internal state to package-internal tests only.
//
//nolint:unused // referenced by service_test.go.
func (s *Service) workerRunningForTest() bool {
	return s.worker.Load() != nil
}
