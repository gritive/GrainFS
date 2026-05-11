package lifecycle

import (
	"context"
	"encoding/xml"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// Proposer is the seam between Service and the cluster meta-Raft proposer.
// internal/cluster provides the adapter.
type Proposer interface {
	ProposeLifecyclePut(ctx context.Context, bucket string, raw []byte) error
	ProposeLifecycleDelete(ctx context.Context, bucket string) error
}

// LeadershipSignal is the seam between Service and the Raft node. It does not
// expose raft.Event so lifecycle does not depend on the raft package.
//
// Subscribe returns a channel that is signalled on every leader-state change.
// The returned cancel func must be called to release the subscription.
type LeadershipSignal interface {
	IsLeader() bool
	Subscribe() (events <-chan struct{}, cancel func())
}

// Service is the deep module for the Bucket Lifecycle Policy domain. It owns
// validation, replication via Proposer, persistence in Store, and the leader-
// only executor. Callers do not reach past this interface.
type Service struct {
	store      *Store
	proposer   Proposer
	leadership LeadershipSignal
	backend    Scrubbable    // for executor; may be nil in unit tests
	deleter    ObjectDeleter // for executor; may be nil in unit tests
	interval   time.Duration

	mu       sync.Mutex
	running  bool
	cancelFn context.CancelFunc
	workerWG sync.WaitGroup

	logger zerolog.Logger
}

// NewService wires the service. backend/deleter may be nil for tests that do
// not exercise Run.
func NewService(store *Store, prop Proposer, lead LeadershipSignal, backend Scrubbable, deleter ObjectDeleter, interval time.Duration) *Service {
	return &Service{
		store:      store,
		proposer:   prop,
		leadership: lead,
		backend:    backend,
		deleter:    deleter,
		interval:   interval,
		logger:     log.With().Str("component", "lifecycle-service").Logger(),
	}
}

// Enabled reports whether the lifecycle service is active. With the service
// wired, it is always true; the boot-disabled case is represented by a nil
// *Service in callers, which is the single source of truth.
func (s *Service) Enabled() bool { return s != nil }

// Get returns the lifecycle configuration for bucket, or (nil, nil) if not set.
func (s *Service) Get(bucket string) (*LifecycleConfiguration, error) {
	return s.store.Get(bucket)
}

// GetRaw returns the raw S3 wire XML bytes for bucket, or (nil, nil) if not
// set. Used by S3 GET handlers to preserve operator round-trip byte-for-byte
// (ADR 0011).
func (s *Service) GetRaw(bucket string) ([]byte, error) {
	return s.store.GetRaw(bucket)
}

// Apply validates a raw S3 wire XML lifecycle configuration and proposes it
// through the meta-Raft FSM. The raw bytes are stored verbatim so GET returns
// byte-for-byte what the operator sent.
func (s *Service) Apply(ctx context.Context, bucket string, raw []byte) error {
	var cfg LifecycleConfiguration
	if err := xml.Unmarshal(raw, &cfg); err != nil {
		return fmt.Errorf("lifecycle: malformed XML: %w", err)
	}
	if len(cfg.Rules) == 0 {
		return fmt.Errorf("lifecycle: configuration must contain at least one rule")
	}
	if err := Validate(&cfg); err != nil {
		return fmt.Errorf("lifecycle: invalid configuration: %w", err)
	}
	return s.proposer.ProposeLifecyclePut(ctx, bucket, raw)
}

// Delete proposes removal of the bucket's lifecycle configuration.
func (s *Service) Delete(ctx context.Context, bucket string) error {
	return s.proposer.ProposeLifecycleDelete(ctx, bucket)
}

// Run watches leadership changes until ctx is done, starting/stopping the
// executor. Ported from cluster.LifecycleManager.Run, adapted to use the
// LeadershipSignal seam so this module does not depend on internal/raft.
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
				// LeadershipSignal closed the channel; treat as terminal.
				s.stop()
				return
			}
			s.reconcile(ctx)
		}
	}
}

func (s *Service) reconcile(ctx context.Context) {
	isLeader := s.leadership.IsLeader()
	s.mu.Lock()
	running := s.running
	s.mu.Unlock()
	switch {
	case isLeader && !running:
		s.start(ctx)
	case !isLeader && running:
		s.stop()
	}
}

func (s *Service) start(parent context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running {
		return
	}
	if s.backend == nil || s.deleter == nil {
		s.logger.Warn().Msg("executor not started: backend/deleter unset")
		return
	}
	workerCtx, cancel := context.WithCancel(parent)
	s.cancelFn = cancel
	s.running = true
	s.workerWG.Add(1)
	w := NewWorker(s.store, s.backend, s.deleter, s.interval)
	go func() {
		defer s.workerWG.Done()
		s.logger.Info().Dur("interval", s.interval).Msg("starting lifecycle executor (now leader)")
		w.Run(workerCtx)
		s.logger.Info().Msg("lifecycle executor stopped")
	}()
}

func (s *Service) stop() {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return
	}
	cancel := s.cancelFn
	s.cancelFn = nil
	s.running = false
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	s.workerWG.Wait()
}

// workerRunningForTest exposes internal state to package-internal tests only.
func (s *Service) workerRunningForTest() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running
}
