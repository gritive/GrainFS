// Package alertssvc owns the dashboard's view of the cluster's alert plumbing
// (degraded tracker + last-failed alert + delivery counters) and the
// /api/admin/alerts/* HTTP surface. It was extracted from internal/server as a
// satellite (closure-rich Deps pattern) so the server god-package no longer
// owns the alerts subsystem directly.
//
// Endpoints (registered via Handler.Register):
//
//	GET  /api/admin/alerts/status   → JSON snapshot for the banner
//	POST /api/admin/alerts/resend   → operator-driven retry of the last
//	                                  failed alert (Force Resend button)
package alertssvc

import (
	"sync/atomic"
	"time"

	"github.com/gritive/GrainFS/internal/alerts"
	"github.com/gritive/GrainFS/internal/metrics"
)

// failureSnapshot is the immutable last-failed-alert record swapped via
// atomic.Pointer so readers (StatusSnapshot, ResendLastFailed) never block.
type failureSnapshot struct {
	Alert      alerts.Alert
	ErrMessage string
	At         time.Time
}

// State owns the dashboard's view of the cluster's alert plumbing:
// degraded tracker + last-failed alert + delivery counters. The Server
// holds a single State and exposes it via /api/admin/alerts/*.
//
// All fields are lock-free: counters are atomic.Uint64, the last-failed slot
// is atomic.Pointer[failureSnapshot] (COW), and secondary callbacks live
// in atomic.Pointer[[]func(bool)]. Dispatcher delivery is fire-and-forget;
// success/failure bookkeeping happens in onResult (controller goroutine).
type State struct {
	dispatcher *alerts.Dispatcher
	tracker    *alerts.DegradedTracker

	deliveredOK    atomic.Uint64
	deliveryFailed atomic.Uint64
	lastFailed     atomic.Pointer[failureSnapshot]

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
func (s *State) AddOnStateChange(fn func(bool)) {
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

// Send fires an alert through the dispatcher in fire-and-forget mode. Returns
// immediately. Delivery success/failure bookkeeping happens asynchronously in
// onResult (invoked from the dispatcher's controller goroutine).
//
// Safe to call from any goroutine.
func (s *State) Send(a alerts.Alert) {
	s.dispatcher.Send(a)
}

// onResult is registered on the dispatcher Options.OnResult. Called from the
// controller goroutine — must not block. atomic stores only.
func (s *State) onResult(a alerts.Alert, err error) {
	if err == nil {
		s.deliveredOK.Add(1)
		metrics.AlertDeliveryAttempts.WithLabelValues("success").Inc()
		return
	}
	s.deliveryFailed.Add(1)
	metrics.AlertDeliveryAttempts.WithLabelValues("failed").Inc()
	metrics.AlertDeliveryFailedTotal.Inc()
	s.lastFailed.Store(&failureSnapshot{
		Alert:      a,
		ErrMessage: err.Error(),
		At:         time.Now(),
	})
}

// Tracker exposes the DegradedTracker for callers (scrubber, raft, disk
// monitor) that need to push fault/healthy reports. The gauge wrapper that
// used to live here was removed; gauge mirroring now happens inside the
// tracker via DegradedConfig.OnStateChange, which is bit-exact-consistent
// with tracker state.
func (s *State) Tracker() *alerts.DegradedTracker {
	return s.tracker
}

// Close stops the DegradedTracker actor goroutine. Call once when the server
// shuts down. Idempotent.
func (s *State) Close() {
	s.tracker.Stop()
}
