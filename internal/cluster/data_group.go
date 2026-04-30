package cluster

import (
	"sort"
	"sync/atomic"
)

// DataGroup is the data Raft group scaffold. PR-D adds raft.Node + FSM.
// bucket→group routing is handled by Router (Layer 1). key-range sharding excluded.
type DataGroup struct {
	id      string
	peerIDs []string
	backend *GroupBackend // nil until wired (per-group dispatch unavailable)
}

// NewDataGroup creates a DataGroup with the given peer list.
func NewDataGroup(id string, peerIDs []string) *DataGroup {
	return &DataGroup{id: id, peerIDs: peerIDs}
}

// NewDataGroupWithBackend creates a DataGroup pre-wired with a GroupBackend.
func NewDataGroupWithBackend(id string, peerIDs []string, b *GroupBackend) *DataGroup {
	return &DataGroup{id: id, peerIDs: peerIDs, backend: b}
}

func (g *DataGroup) ID() string { return g.id }

// PeerIDs returns the peer list for this group.
// The returned slice is read-only; callers must not modify or append to it.
func (g *DataGroup) PeerIDs() []string { return g.peerIDs }

// Backend returns the GroupBackend wired to this group, or nil if not yet set.
func (g *DataGroup) Backend() *GroupBackend { return g.backend }

// groupSnapshot is an immutable snapshot of DataGroupManager. COW replacement enables lock-free reads.
type groupSnapshot struct {
	byID map[string]*DataGroup
	all  []*DataGroup // sorted by ID (deterministic iteration)
}

// DataGroupManager manages a set of DataGroups with lock-free reads (atomic.Pointer COW).
// Writes (Add) are infrequent (rebalance events), so a CAS loop is sufficient.
type DataGroupManager struct {
	snap atomic.Pointer[groupSnapshot]
}

func NewDataGroupManager() *DataGroupManager {
	m := &DataGroupManager{}
	m.snap.Store(&groupSnapshot{byID: make(map[string]*DataGroup)})
	return m
}

// Add adds a DataGroup or replaces an existing group with the same ID. Thread-safe.
func (m *DataGroupManager) Add(g *DataGroup) {
	for {
		old := m.snap.Load()
		newByID := make(map[string]*DataGroup, len(old.byID)+1)
		for k, v := range old.byID {
			newByID[k] = v
		}
		newByID[g.id] = g

		all := make([]*DataGroup, 0, len(newByID))
		for _, dg := range newByID {
			all = append(all, dg)
		}
		sort.Slice(all, func(i, j int) bool { return all[i].id < all[j].id })

		newSnap := &groupSnapshot{byID: newByID, all: all}
		if m.snap.CompareAndSwap(old, newSnap) {
			return
		}
	}
}

// Get returns the DataGroup for the given id, or nil if not found.
// Read path: lock-free (uses only atomic.Pointer.Load).
func (m *DataGroupManager) Get(id string) *DataGroup {
	return m.snap.Load().byID[id]
}

// All returns the current slice of DataGroups sorted by ID.
func (m *DataGroupManager) All() []*DataGroup {
	return m.snap.Load().all
}

// Remove removes the group with the given ID. Returns true if removed.
// Used during graceful shutdown when a node leaves a voter set.
func (m *DataGroupManager) Remove(id string) bool {
	for {
		old := m.snap.Load()
		if _, ok := old.byID[id]; !ok {
			return false
		}
		newByID := make(map[string]*DataGroup, len(old.byID)-1)
		for k, v := range old.byID {
			if k != id {
				newByID[k] = v
			}
		}
		all := make([]*DataGroup, 0, len(newByID))
		for _, dg := range newByID {
			all = append(all, dg)
		}
		sort.Slice(all, func(i, j int) bool { return all[i].id < all[j].id })
		newSnap := &groupSnapshot{byID: newByID, all: all}
		if m.snap.CompareAndSwap(old, newSnap) {
			return true
		}
	}
}
