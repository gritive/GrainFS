// Phase 16 Week 4 — alert state surfaced to the dashboard banner.
//
// Endpoints (registered in server.go):
//
//	GET  /api/admin/alerts/status   → JSON snapshot for the banner
//	POST /api/admin/alerts/resend   → operator-driven retry of the last
//	                                  failed alert (Force Resend button)
package server

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/gritive/GrainFS/internal/alerts"
	"github.com/gritive/GrainFS/internal/metrics"
)

// AlertsState owns the dashboard's view of the cluster's alert plumbing:
// degraded tracker + last-failed alert + delivery counters. The Server
// holds a single AlertsState and exposes it via /api/admin/alerts/*.
type AlertsState struct {
	dispatcher *alerts.Dispatcher
	tracker    *alerts.DegradedTracker

	mu             sync.Mutex
	lastFailed     *alerts.Alert
	lastFailedErr  string
	lastFailedAt   time.Time
	deliveredOK    uint64
	deliveryFailed uint64

	// Secondary OnStateChange callbacks wired after construction (e.g. by Server).
	// Lock-free: append-only via CompareAndSwap, snapshot-read in the tracker's
	// actor goroutine. Callbacks must not block or call back into the tracker.
	secondaryCallbacks atomic.Pointer[[]func(bool)]
}

// AddOnStateChange registers a callback that is invoked whenever the degraded
// state changes. Callbacks run inside the tracker's actor goroutine and must
// not block or call back into the tracker (deadlock risk).
// Safe to call at any time, including after the tracker has started.
//
// Lock-free CAS: read the current slice, build next, swap; retry if another
// caller raced. In practice all registrations happen at startup so the loop
// rarely iterates more than once.
func (s *AlertsState) AddOnStateChange(fn func(bool)) {
	for {
		cur := s.secondaryCallbacks.Load()
		var existing []func(bool)
		if cur != nil {
			existing = *cur
		}
		next := make([]func(bool), len(existing)+1)
		copy(next, existing)
		next[len(existing)] = fn
		if s.secondaryCallbacks.CompareAndSwap(cur, &next) {
			return
		}
	}
}

// Send fires an alert through the dispatcher and updates counters/metrics.
// Safe to call from any goroutine.
func (s *AlertsState) Send(a alerts.Alert) error {
	err := s.dispatcher.Send(a)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		s.deliveryFailed++
		metrics.AlertDeliveryAttempts.WithLabelValues("failed").Inc()
		return err
	}
	s.deliveredOK++
	metrics.AlertDeliveryAttempts.WithLabelValues("success").Inc()
	return nil
}

// Tracker exposes the DegradedTracker for callers (scrubber, raft, disk
// monitor) that need to push fault/healthy reports. The gauge wrapper that
// used to live here was removed; gauge mirroring now happens inside the
// tracker via DegradedConfig.OnStateChange, which is bit-exact-consistent
// with tracker state.
func (s *AlertsState) Tracker() *alerts.DegradedTracker {
	return s.tracker
}

// Close stops the DegradedTracker actor goroutine. Call once when the server
// shuts down. Idempotent.
func (s *AlertsState) Close() {
	s.tracker.Stop()
}

// Alerts returns the AlertsState wired into this server, or nil if alerts
// were not configured. Used by other server components (raft monitor, disk
// collector) to push fault/healthy reports.
func (s *Server) Alerts() *AlertsState { return s.alerts }

func (s *AlertsState) recordFailure(a alerts.Alert, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastFailed = &a
	s.lastFailedErr = err.Error()
	s.lastFailedAt = time.Now()
	metrics.AlertDeliveryFailedTotal.Inc()
}
