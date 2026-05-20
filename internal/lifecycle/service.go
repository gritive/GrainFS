package lifecycle

import (
	"context"
	"encoding/xml"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/time/rate"
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
//
// See docs/adr/0013-lifecycle-service-lock-free-publication.md for why the
// worker handle is published lock-free instead of through a controller actor.
type Service struct {
	store      *Store
	proposer   Proposer
	leadership LeadershipSignal
	backend    Scrubbable    // for executor; may be nil in unit tests
	deleter    ObjectDeleter // for executor; may be nil in unit tests
	interval   time.Duration
	// nodeID labels per-node MPU metrics. Empty string when unset (unit
	// tests, single-node bootstraps that haven't generated a node ID yet).
	nodeID string

	// worker is published by Run()'s reconcile loop. Status loads it
	// lock-free; nil means this node is not the current leader.
	worker atomic.Pointer[Worker]

	// cancelFn and workerWG are only touched by the Run() goroutine.
	cancelFn context.CancelFunc
	workerWG sync.WaitGroup

	// mpuWorker runs per-node (every node, always on) for AbortIncompleteMPU.
	// Independent of leadership — split execution model.
	mpuWorker atomic.Pointer[MPUWorker]
	mpuCancel context.CancelFunc
	mpuWG     sync.WaitGroup

	// limiter is shared by both workers so the 100 deletes/sec/node cap
	// holds across both object-side and MPU-side clauses.
	limiter *rate.Limiter

	logger zerolog.Logger
}

// NewService wires the service. backend/deleter may be nil for tests that do
// not exercise Run.
func NewService(store *Store, prop Proposer, lead LeadershipSignal, backend Scrubbable, deleter ObjectDeleter, interval time.Duration, opts ...ServiceOption) *Service {
	s := &Service{
		store:      store,
		proposer:   prop,
		leadership: lead,
		backend:    backend,
		deleter:    deleter,
		interval:   interval,
		limiter:    rate.NewLimiter(100, 10), // 100 deletes/sec/node, burst 10 — shared by both workers
		logger:     log.With().Str("component", "lifecycle-service").Logger(),
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// ServiceOption configures a Service at construction.
type ServiceOption func(*Service)

// WithNodeID sets the node identifier propagated to the per-node MPU worker
// for metric labelling.
func WithNodeID(nodeID string) ServiceOption {
	return func(s *Service) { s.nodeID = nodeID }
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

// Status is a point-in-time view of the lifecycle executor for the
// /api/cluster/lifecycle/status admin endpoint. When the node is not the
// leader the object-side executor is not running and its counters are
// zero; the per-node MPU worker runs on every node so MPUWorkerRunning /
// AbortedUploads are populated independently of leadership.
type Status struct {
	Running                bool      `json:"running"`
	MPUWorkerRunning       bool      `json:"mpu_worker_running"`
	LastRun                time.Time `json:"last_run,omitempty"`
	LastCycleSeconds       float64   `json:"last_cycle_seconds"`
	ObjectsChecked         int64     `json:"objects_checked"`
	Expired                int64     `json:"expired"`
	VersionsPruned         int64     `json:"versions_pruned"`
	AbortedUploads         int64     `json:"aborted_uploads"`
	DeleteMarkersReclaimed int64     `json:"delete_markers_reclaimed"`
	Buckets                []string  `json:"buckets"` // buckets with a lifecycle config persisted locally
}

// Status returns the current executor status. Safe to call on any node; a
// follower returns Status{Running: false} but still surfaces MPU-side
// fields populated by the per-node MPU worker.
func (s *Service) Status() Status {
	w := s.worker.Load()
	mpu := s.mpuWorker.Load()
	buckets, err := s.store.ListBuckets()
	if err != nil {
		s.logger.Warn().Err(err).Msg("lifecycle status: ListBuckets failed")
	}
	if buckets == nil {
		buckets = []string{}
	}
	st := Status{
		MPUWorkerRunning: mpu != nil,
		Buckets:          buckets,
	}
	if mpu != nil {
		st.AbortedUploads = mpu.AbortedTotal()
	}
	if w != nil {
		ws := w.Stats()
		st.Running = true
		st.LastRun = ws.LastRun
		st.LastCycleSeconds = ws.LastCycleSeconds
		st.ObjectsChecked = ws.ObjectsChecked
		st.Expired = ws.Expired
		st.VersionsPruned = ws.VersionsPruned
		st.DeleteMarkersReclaimed = ws.DeleteMarkersReclaimed
	}
	return st
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
	// MPU worker is per-node, always on: starts on every node regardless of
	// leadership (split execution model — see spec § "Cluster / Raft").
	s.startMPUWorker(ctx)

	events, cancel := s.leadership.Subscribe()
	defer cancel()

	s.reconcile(ctx)

	for {
		select {
		case <-ctx.Done():
			s.stop()
			s.stopMPUWorker()
			return
		case _, ok := <-events:
			if !ok {
				// LeadershipSignal closed the channel; treat as terminal.
				s.stop()
				s.stopMPUWorker()
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
		if buckets, err := s.store.ListBuckets(); err != nil {
			s.logger.Warn().Err(err).Msg("lifecycle: could not audit local config keys on leadership acquire")
		} else {
			s.logger.Info().Strs("buckets", buckets).Int("count", len(buckets)).
				Msg("lifecycle configs present in local store at leadership acquire")
		}
		s.start(ctx)
	case !isLeader && running:
		s.stop()
	}
}

func (s *Service) start(parent context.Context) {
	if s.worker.Load() != nil {
		return
	}
	if s.backend == nil || s.deleter == nil {
		s.logger.Warn().Msg("executor not started: backend/deleter unset")
		return
	}
	workerCtx, cancel := context.WithCancel(parent)
	w := NewWorker(s.store, s.backend, s.deleter, s.interval, s.limiter)
	s.cancelFn = cancel
	s.worker.Store(w)
	s.workerWG.Add(1)
	go func() {
		defer s.workerWG.Done()
		s.logger.Info().Dur("interval", s.interval).Msg("starting lifecycle executor (now leader)")
		w.Run(workerCtx)
		s.logger.Info().Msg("lifecycle executor stopped")
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

// startMPUWorker spins up the per-node MPUWorker. Idempotent: a second call
// while the worker is running is a no-op. Independent of leadership.
func (s *Service) startMPUWorker(parent context.Context) {
	if s.mpuWorker.Load() != nil {
		return
	}
	if s.backend == nil || s.deleter == nil {
		s.logger.Warn().Msg("MPU worker not started: backend/deleter unset")
		return
	}
	workerCtx, cancel := context.WithCancel(parent)
	w := NewMPUWorker(s.store, s.backend, s.deleter, s.interval, s.limiter, WithMPUNodeID(s.nodeID))
	s.mpuCancel = cancel
	s.mpuWorker.Store(w)
	s.mpuWG.Add(1)
	go func() {
		defer s.mpuWG.Done()
		s.logger.Info().Dur("interval", s.interval).Msg("starting lifecycle MPU worker (per-node, always on)")
		w.Run(workerCtx)
		s.logger.Info().Msg("lifecycle MPU worker stopped")
	}()
}

func (s *Service) stopMPUWorker() {
	if s.mpuWorker.Load() == nil {
		return
	}
	cancel := s.mpuCancel
	s.mpuCancel = nil
	s.mpuWorker.Store(nil)
	if cancel != nil {
		cancel()
	}
	s.mpuWG.Wait()
}

// WorkerRunningForTest exposes whether the leader-only object worker is
// running. Test seam.
//
//nolint:unused // referenced by service_test.go.
func (s *Service) WorkerRunningForTest() bool {
	return s.worker.Load() != nil
}

// MPUWorkerRunningForTest exposes whether the per-node MPU worker is
// running. Test seam.
//
//nolint:unused // referenced by service_test.go.
func (s *Service) MPUWorkerRunningForTest() bool {
	return s.mpuWorker.Load() != nil
}

// RunCycleForTest synchronously runs one object-side cycle on the loaded
// worker, if any. Followers (worker not loaded) return immediately. The MPU
// worker is per-node and always loaded once Run has started, so we also
// drive one MPU cycle in the same call. Test seam.
func (s *Service) RunCycleForTest(ctx context.Context) {
	w := s.worker.Load()
	if w != nil {
		w.RunCycleForTest(ctx)
	}
	if mpu := s.mpuWorker.Load(); mpu != nil {
		mpu.RunCycleForTest(ctx)
	}
}

// SetNowForTest reconfigures the clock source on any currently-loaded
// workers. Called after Run has started. Test seam.
func (s *Service) SetNowForTest(now func() time.Time) {
	if w := s.worker.Load(); w != nil {
		w.SetNowForTest(now)
	}
	if mpu := s.mpuWorker.Load(); mpu != nil {
		mpu.SetNowForTest(now)
	}
}
