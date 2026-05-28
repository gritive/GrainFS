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

// LeaderIDs returns the currently known raft leader for locally instantiated
// data groups. Placeholder groups and groups whose raft node has not observed a
// leader yet are omitted.
func (m *DataGroupManager) LeaderIDs() map[string]string {
	snap := m.snap.Load()
	out := make(map[string]string, len(snap.all))
	for _, dg := range snap.all {
		if dg == nil || dg.backend == nil || dg.backend.Node() == nil {
			continue
		}
		leaderID := dg.backend.Node().LeaderID()
		if leaderID == "" {
			continue
		}
		out[dg.id] = leaderID
	}
	return out
}

// DataGroupRaftHealth is an immutable operator-facing snapshot of one data
// group's local Raft progress. Server packages map this internal shape to
// closed wire structs in adminapi.
type DataGroupRaftHealth struct {
	GroupID        string
	PeerIDs        []string
	LocalState     string
	LeaderID       string
	Term           uint64
	CommitIndex    uint64
	LastLogIndex    uint64
	PeerMatchIndex map[string]uint64
	MaxPeerLag     uint64
	Issues         []string
}

// RaftHealthSnapshot returns a deterministic snapshot of every known data
// group. Placeholder/unwired groups are included so operators can see topology
// that has not reached the data-Raft layer yet.
func (m *DataGroupManager) RaftHealthSnapshot() []DataGroupRaftHealth {
	snap := m.snap.Load()
	out := make([]DataGroupRaftHealth, 0, len(snap.all))
	for _, dg := range snap.all {
		if dg == nil {
			continue
		}
		row := DataGroupRaftHealth{
			GroupID: dg.id,
			PeerIDs: append([]string(nil), dg.peerIDs...),
		}
		if dg.backend == nil || dg.backend.Node() == nil {
			row.Issues = append(row.Issues, "unwired")
			out = append(out, row)
			continue
		}
		node := dg.backend.Node()
		row.LocalState = node.State().String()
		row.LeaderID = node.LeaderID()
		row.Term = node.Term()
		row.CommitIndex = node.CommittedIndex()
		row.LastLogIndex = node.LastLogIndex()
		if row.LeaderID == "" {
			row.Issues = append(row.Issues, "leaderless")
		}
		if len(dg.peerIDs) > 0 {
			row.PeerMatchIndex = make(map[string]uint64, len(dg.peerIDs))
			for _, peer := range dg.peerIDs {
				match, ok := node.PeerMatchIndex(peer)
				if !ok {
					continue
				}
				row.PeerMatchIndex[peer] = match
				if row.CommitIndex > match {
					lag := row.CommitIndex - match
					if lag > row.MaxPeerLag {
						row.MaxPeerLag = lag
					}
				}
			}
			if row.MaxPeerLag > 0 {
				row.Issues = append(row.Issues, "peer_lag")
			}
		}
		out = append(out, row)
	}
	return out
}

// GroupForBucket resolves a bucket to its DataGroup via the supplied router.
// Returns (nil, false) if router is nil, the bucket has no assignment and no
// default group is set, or the assigned group has been removed from the manager.
//
// This is the ClusterCoordinator's single entry point for bucket-scoped routing
// — keeping the lookup in one place avoids drift between coordinator code paths.
func (m *DataGroupManager) GroupForBucket(bucket string, router *Router) (*DataGroup, bool) {
	if router == nil {
		return nil, false
	}
	dg, err := router.RouteKey(bucket, "")
	if err != nil || dg == nil {
		return nil, false
	}
	return dg, true
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
