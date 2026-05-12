package cluster

import "sync"

// circuitBreaker is a 2-state (open/closed) gate per destination node.
// open = disk is full, reject writes; closed = allow writes.
//
// Threshold is NOT stored on the breaker — it lives in ClusterConfig
// (single source of truth, hot-reloadable). update() takes the threshold
// per call so a cluster_config PATCH reaches existing breakers on the next
// syncCB tick (state minimization: threshold is a derived value, not cached).
type circuitBreaker struct {
	mu   sync.RWMutex
	open bool
}

func newCircuitBreaker() *circuitBreaker { return &circuitBreaker{} }

// update recalculates open/closed from the latest gossip stats against the
// caller-supplied threshold (percent, e.g. 85.0 for 85%).
func (cb *circuitBreaker) update(ns NodeStats, thresholdPct float64) {
	cb.mu.Lock()
	cb.open = ns.DiskUsedPct >= thresholdPct
	cb.mu.Unlock()
}

// allow returns false when the circuit is open (disk full), true otherwise.
func (cb *circuitBreaker) allow() bool {
	cb.mu.RLock()
	v := !cb.open
	cb.mu.RUnlock()
	return v
}
