package cluster

import "sync"

// IndexGroupManager owns the local object-index raft groups inside the cluster
// package. indexGroup is unexported, so serveruntime (a different package)
// cannot hold a map[string]*indexGroup directly — it touches only the exported
// *IndexGroupManager. Mirrors DataGroupManager's ownership role for data groups.
//
// This slice (Task 4.5) introduces the map + Lookup so the index-group proposal
// forward receiver can resolve a forwarded group ID to its local group. Task 5
// extends the manager with instantiate/start/Shards/Close.
type IndexGroupManager struct {
	mu     sync.RWMutex
	groups map[string]*indexGroup
	// closes holds each group's v2-close func (used by Task 5's Close). Kept
	// keyed alongside groups so registration is a single atomic step.
	closes map[string]func() error
}

func NewIndexGroupManager() *IndexGroupManager {
	return &IndexGroupManager{
		groups: make(map[string]*indexGroup),
		closes: make(map[string]func() error),
	}
}

// Lookup returns the local index group for groupID, or (nil, false) when no
// group is registered (e.g. during staggered boot before the group is wired).
func (m *IndexGroupManager) Lookup(groupID string) (*indexGroup, bool) {
	m.mu.RLock()
	g, ok := m.groups[groupID]
	m.mu.RUnlock()
	return g, ok
}

// register installs a local index group under groupID with its close func.
// Unexported: only the boot wiring (Task 5) registers groups; tests use it to
// stand up the manager. Last-writer-wins on a key.
//
//nolint:unused // Slice 4b-1 primitive — wired by Task 5's boot phase; exercised today by index_group_forward_test.
func (m *IndexGroupManager) register(groupID string, g *indexGroup, closeFn func() error) {
	m.mu.Lock()
	m.groups[groupID] = g
	m.closes[groupID] = closeFn
	m.mu.Unlock()
}
