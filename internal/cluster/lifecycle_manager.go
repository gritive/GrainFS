package cluster

// Slice 4 of refactor/unify-storage-paths: leader-only lifecycle worker.
//
// In cluster mode only the Raft leader may run lifecycle. Followers would
// double-propose the same Delete/DeleteVersion entries; the proposals would
// all be rejected by Raft (only the leader can propose), but the scan itself
// still costs IO on every follower. Running exclusively on the leader avoids
// the wasted work and keeps deletes linearised.

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/gritive/GrainFS/internal/lifecycle"
	"github.com/gritive/GrainFS/internal/raft"
)

// leadershipSource is the minimum surface the manager needs from a Raft node.
// Keeping it as an interface makes the manager trivially testable without a
// real raft.Node — unit tests swap in a value that toggles Leader/Follower.
type leadershipSource interface {
	State() raft.NodeState
}

// LifecycleManager starts/stops the lifecycle worker based on Raft leadership.
// Only the leader runs the worker to avoid double-deletes across the cluster.
type LifecycleManager struct {
	leadership leadershipSource
	store      *lifecycle.Store
	worker     *lifecycle.Worker
	interval   time.Duration
	pollEvery  time.Duration

	mu       sync.Mutex
	running  bool
	cancelFn context.CancelFunc
	workerWG sync.WaitGroup

	logger *slog.Logger
}

// Store returns the lifecycle config store shared with the worker.
// Callers wire this into the S3 server so PutBucketLifecycle proposals land in
// the same BadgerDB the worker scans.
func (m *LifecycleManager) Store() *lifecycle.Store { return m.store }

// NewLifecycleManager wires a lifecycle worker over the given DistributedBackend.
// The worker's Store uses the shared FSM BadgerDB, and the DistributedBackend
// itself is the ObjectDeleter (it already satisfies the interface after Slice 1).
func NewLifecycleManager(backend *DistributedBackend, interval time.Duration) *LifecycleManager {
	store := lifecycle.NewStore(backend.FSMDB())
	worker := lifecycle.NewWorker(store, backend, backend, interval)
	return &LifecycleManager{
		leadership: backend.node,
		store:      store,
		worker:     worker,
		interval:   interval,
		pollEvery:  250 * time.Millisecond,
		logger:     slog.With("component", "lifecycle-manager"),
	}
}

// Run watches leadership changes until ctx is done, starting/stopping the worker.
// Poll cadence is 250ms — cheap enough to be ignorable and tight enough to catch
// a leadership flip within one heartbeat interval.
func (m *LifecycleManager) Run(ctx context.Context) {
	ticker := time.NewTicker(m.pollEvery)
	defer ticker.Stop()

	// Evaluate immediately so a node that is already leader at startup picks
	// the worker up without waiting a full poll interval.
	m.reconcile(ctx)

	for {
		select {
		case <-ctx.Done():
			m.stop()
			return
		case <-ticker.C:
			m.reconcile(ctx)
		}
	}
}

// reconcile brings the worker state in line with the current Raft role.
func (m *LifecycleManager) reconcile(ctx context.Context) {
	isLeader := m.leadership.State() == raft.Leader

	m.mu.Lock()
	running := m.running
	m.mu.Unlock()

	switch {
	case isLeader && !running:
		m.start(ctx)
	case !isLeader && running:
		m.stop()
	}
}

// start spawns the worker goroutine. Must be called with m.running == false.
func (m *LifecycleManager) start(parent context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.running {
		// Another goroutine raced us here — be a no-op. start/stop are
		// serialised by reconcile in practice, but the lock makes the
		// invariant explicit.
		return
	}
	workerCtx, cancel := context.WithCancel(parent)
	m.cancelFn = cancel
	m.running = true
	m.workerWG.Add(1)
	go func() {
		defer m.workerWG.Done()
		m.logger.Info("starting lifecycle worker (now leader)", "interval", m.interval)
		m.worker.Run(workerCtx)
		m.logger.Info("lifecycle worker stopped")
	}()
}

// stop cancels the worker goroutine and waits for it to exit.
// Safe to call when not running.
func (m *LifecycleManager) stop() {
	m.mu.Lock()
	if !m.running {
		m.mu.Unlock()
		return
	}
	cancel := m.cancelFn
	m.cancelFn = nil
	m.running = false
	m.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	m.workerWG.Wait()
}
