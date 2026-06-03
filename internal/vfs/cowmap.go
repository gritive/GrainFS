package vfs

import (
	"sync"
	"sync/atomic"
)

// cowMap is a copy-on-write string-keyed map: readers take a zero-lock snapshot
// via load(), writers clone-and-replace under a thin write mutex via update().
// It concentrates the Load → Lock → reLoad → clone → Store atomic-CoW protocol
// that the VFS stat/dir caches previously hand-copied at every mutation site.
//
// The zero value is a disabled cache: the internal pointer is nil, load()
// returns nil, and update() is a no-op. enable() activates it (called once when
// the corresponding TTL is positive). The cache is never disabled again, so a
// nil check outside the lock is stable.
type cowMap[V any] struct {
	ptr atomic.Pointer[map[string]V]
	mu  sync.Mutex
}

// enable activates the cache with an empty backing map. Idempotent callers
// should guard on their TTL; enable itself simply installs a fresh map.
func (c *cowMap[V]) enable() {
	m := make(map[string]V)
	c.ptr.Store(&m)
}

// load returns the current snapshot, or nil if the cache is disabled. Readers
// must treat the returned map as immutable.
func (c *cowMap[V]) load() map[string]V {
	p := c.ptr.Load()
	if p == nil {
		return nil
	}
	return *p
}

// update clones-and-replaces the map under the write lock. fn receives the
// current map and returns its replacement; the caller's fn owns the clone (so
// it can pick the capacity hint and skip cloning entirely). Returning nil
// aborts the store — no new map is published and readers keep the existing
// snapshot, which lets callers avoid an allocation when there is nothing to do.
// A disabled cache is a no-op.
func (c *cowMap[V]) update(fn func(cur map[string]V) map[string]V) {
	p := c.ptr.Load()
	if p == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	p = c.ptr.Load()
	if next := fn(*p); next != nil {
		c.ptr.Store(&next)
	}
}
