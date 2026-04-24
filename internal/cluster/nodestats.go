package cluster

import (
	"maps"
	"sync"
	"sync/atomic"
	"time"
)

// NodeStats holds a snapshot of a node's resource usage, received via gossip.
type NodeStats struct {
	NodeID         string
	DiskUsedPct    float64
	DiskAvailBytes uint64
	RequestsPerSec float64
	JoinedAt       time.Time // when this node joined the cluster (zero = unknown)
	UpdatedAt      time.Time
}

// NodeStatsStore is a thread-safe, TTL-based in-memory store for per-node stats.
//
// Hot path (Get / GetAll) touches zero mutexes: it loads an atomic.Pointer
// snapshot and reads an immutable map. Writers serialise on writeMu and
// publish a new CoW snapshot via Store.
type NodeStatsStore struct {
	snap    atomic.Pointer[map[string]NodeStats] // read via Load(), no lock
	writeMu sync.Mutex                           // serialises writers
	ttl     time.Duration
}

// NewNodeStatsStore creates a store where entries expire after ttl.
func NewNodeStatsStore(ttl time.Duration) *NodeStatsStore {
	s := &NodeStatsStore{ttl: ttl}
	empty := make(map[string]NodeStats)
	s.snap.Store(&empty)
	return s
}

// Set stores stats for a node, stamping UpdatedAt with now.
// Expired entries are purged on every Set to prevent unbounded map growth.
// Invalid ranges are clamped: DiskUsedPct→[0,100], RequestsPerSec→[0,∞).
func (s *NodeStatsStore) Set(ns NodeStats) {
	if ns.DiskUsedPct < 0 {
		ns.DiskUsedPct = 0
	} else if ns.DiskUsedPct > 100 {
		ns.DiskUsedPct = 100
	}
	if ns.RequestsPerSec < 0 {
		ns.RequestsPerSec = 0
	}
	now := time.Now()
	ns.UpdatedAt = now

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	cur := *s.snap.Load()
	next := make(map[string]NodeStats, len(cur)+1)
	for id, existing := range cur {
		if now.Sub(existing.UpdatedAt) <= s.ttl {
			next[id] = existing
		}
	}
	next[ns.NodeID] = ns
	s.snap.Store(&next)
}

// Get returns the stats for nodeID. Returns false if the entry is absent or expired.
func (s *NodeStatsStore) Get(nodeID string) (NodeStats, bool) {
	ns, ok := (*s.snap.Load())[nodeID]
	if !ok || time.Since(ns.UpdatedAt) > s.ttl {
		return NodeStats{}, false
	}
	return ns, true
}

// GetAll returns all non-expired entries.
func (s *NodeStatsStore) GetAll() []NodeStats {
	snap := *s.snap.Load()
	now := time.Now()
	out := make([]NodeStats, 0, len(snap))
	for _, ns := range snap {
		if now.Sub(ns.UpdatedAt) <= s.ttl {
			out = append(out, ns)
		}
	}
	return out
}

// UpdateDiskStats atomically updates only the disk fields for nodeID, preserving all other fields.
// No-op if nodeID is not in the store. Clamps usedPct to [0, 100].
func (s *NodeStatsStore) UpdateDiskStats(nodeID string, usedPct float64, availBytes uint64) {
	if usedPct < 0 {
		usedPct = 0
	} else if usedPct > 100 {
		usedPct = 100
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	cur := *s.snap.Load()
	ns, ok := cur[nodeID]
	if !ok {
		return
	}
	ns.DiskUsedPct = usedPct
	ns.DiskAvailBytes = availBytes
	ns.UpdatedAt = time.Now()

	next := make(map[string]NodeStats, len(cur))
	maps.Copy(next, cur)
	next[nodeID] = ns
	s.snap.Store(&next)
}

// Len returns the count of non-expired entries.
func (s *NodeStatsStore) Len() int {
	snap := *s.snap.Load()
	now := time.Now()
	count := 0
	for _, ns := range snap {
		if now.Sub(ns.UpdatedAt) <= s.ttl {
			count++
		}
	}
	return count
}
