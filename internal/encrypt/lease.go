package encrypt

import (
	"sync"
	"sync/atomic"
)

// KEKLeaseTracker tracks in-flight KEK consumers per version. Lock-free counter
// per version (atomic.Uint64); RWMutex on the version map for create-once
// semantics.
//
// Phase B has no real acquire sites. Phase D wires raft snapshot readers
// and InstallSnapshot receivers as actual consumers.
type KEKLeaseTracker struct {
	mu       sync.RWMutex
	counters map[uint32]*atomic.Uint64
}

// NewKEKLeaseTracker returns an empty tracker ready for use.
func NewKEKLeaseTracker() *KEKLeaseTracker {
	return &KEKLeaseTracker{counters: make(map[uint32]*atomic.Uint64)}
}

// Acquire increments the lease counter for version and returns a release
// function. The release function must be called exactly once; calling it twice
// panics ("double release"). Releasing more times than acquired (counter wrap)
// also panics.
func (t *KEKLeaseTracker) Acquire(version uint32) func() {
	// Fast path: version already seen — read lock only.
	t.mu.RLock()
	c, ok := t.counters[version]
	t.mu.RUnlock()
	if !ok {
		// Slow path: first acquire for this version — create-once under write lock.
		t.mu.Lock()
		if c, ok = t.counters[version]; !ok {
			c = &atomic.Uint64{}
			t.counters[version] = c
		}
		t.mu.Unlock()
	}
	c.Add(1)

	var released atomic.Bool
	return func() {
		if !released.CompareAndSwap(false, true) {
			panic("KEKLeaseTracker: double release")
		}
		if c.Add(^uint64(0)) > 1<<62 {
			// Counter wrapped — more releases than acquires.
			panic("KEKLeaseTracker: under-release detected (counter wrapped)")
		}
	}
}

// Count returns the current in-flight lease count for version.
// Returns 0 for unknown versions.
func (t *KEKLeaseTracker) Count(version uint32) uint64 {
	t.mu.RLock()
	c, ok := t.counters[version]
	t.mu.RUnlock()
	if !ok {
		return 0
	}
	return c.Load()
}

// Snapshot returns a defensive copy of all counters keyed by version.
func (t *KEKLeaseTracker) Snapshot() map[uint32]uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make(map[uint32]uint64, len(t.counters))
	for v, c := range t.counters {
		out[v] = c.Load()
	}
	return out
}
