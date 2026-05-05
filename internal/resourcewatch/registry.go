package resourcewatch

import (
	"sync"
	"sync/atomic"

	badger "github.com/dgraph-io/badger/v4"
)

// RegisteredDB carries per-DB GC state for the vlog watcher. consecutiveGCFailures
// and incidentFired are touched by the GC ticker (gc_ticker.go); Category, Dir,
// and DB are immutable after Register.
type RegisteredDB struct {
	Category              Category
	Dir                   string
	DB                    *badger.DB
	consecutiveGCFailures atomic.Int32
	incidentFired         atomic.Bool
}

// ConsecutiveGCFailures returns the live retry counter — used by the admin
// breakdown endpoint to surface per-category GC state.
func (e *RegisteredDB) ConsecutiveGCFailures() int32 {
	return e.consecutiveGCFailures.Load()
}

// Registry tracks live BadgerDB handles for vlog sampling and GC. Tests
// instantiate their own via NewRegistry; production code uses Default.
type Registry struct {
	mu      sync.RWMutex
	entries []*RegisteredDB
}

// Default is the package-level singleton used by RegisterDB / DeregisterDB.
var Default = &Registry{}

// RegisterDB registers db under category in Default. Panics on nil db.
func RegisterDB(category Category, db *badger.DB) *RegisteredDB {
	return Default.Register(category, db)
}

// DeregisterDB removes entry from Default.
func DeregisterDB(entry *RegisteredDB) { Default.Deregister(entry) }

// NewRegistry returns a fresh, empty Registry — intended for tests so each
// test owns isolated state instead of sharing Default.
func NewRegistry() *Registry { return &Registry{} }

// Register adds db under category. Duplicate registrations (same db twice)
// append two entries; that's caller responsibility. Panics on nil db.
func (r *Registry) Register(category Category, db *badger.DB) *RegisteredDB {
	if db == nil {
		panic("resourcewatch: Register nil DB")
	}
	e := &RegisteredDB{Category: category, Dir: db.Opts().Dir, DB: db}
	r.mu.Lock()
	r.entries = append(r.entries, e)
	r.mu.Unlock()
	return e
}

// Deregister removes entry by pointer identity. No-op if not found.
func (r *Registry) Deregister(entry *RegisteredDB) {
	if entry == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, e := range r.entries {
		if e == entry {
			r.entries = append(r.entries[:i], r.entries[i+1:]...)
			return
		}
	}
}

// Snapshot returns a defensive copy of the entries slice under RLock. Callers
// iterate without holding any registry lock so GC duration cannot block
// Register or Deregister (Arch #1 plan-eng-review).
func (r *Registry) Snapshot() []*RegisteredDB {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*RegisteredDB, len(r.entries))
	copy(out, r.entries)
	return out
}

// Reset clears all entries — test isolation only.
func (r *Registry) Reset() {
	r.mu.Lock()
	r.entries = nil
	r.mu.Unlock()
}
