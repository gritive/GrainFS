package cluster

import "sync"

// circuitBreaker is a 2-state (open/closed) gate per destination node.
// open = disk is full, reject writes; closed = allow writes.
// threshold is in percent (e.g. 0.90 means 90%).
type circuitBreaker struct {
	mu        sync.RWMutex
	open      bool
	threshold float64 // percent threshold [0,100) — open when DiskUsedPct >= threshold*100
}

func newCircuitBreaker(thresholdFraction float64) *circuitBreaker {
	return &circuitBreaker{threshold: thresholdFraction * 100}
}

// update recalculates open/closed from the latest gossip stats.
func (cb *circuitBreaker) update(ns NodeStats) {
	cb.mu.Lock()
	cb.open = ns.DiskUsedPct >= cb.threshold
	cb.mu.Unlock()
}

// allow returns false when the circuit is open (disk full), true otherwise.
func (cb *circuitBreaker) allow() bool {
	cb.mu.RLock()
	v := !cb.open
	cb.mu.RUnlock()
	return v
}
