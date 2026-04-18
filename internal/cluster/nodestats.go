package cluster

import (
	"sync"
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
// Entries expire after the configured TTL and are lazily evicted on access.
type NodeStatsStore struct {
	mu    sync.RWMutex
	stats map[string]NodeStats
	ttl   time.Duration
}

// NewNodeStatsStore creates a store where entries expire after ttl.
func NewNodeStatsStore(ttl time.Duration) *NodeStatsStore {
	return &NodeStatsStore{
		stats: make(map[string]NodeStats),
		ttl:   ttl,
	}
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
	s.mu.Lock()
	s.stats[ns.NodeID] = ns
	for id, existing := range s.stats {
		if now.Sub(existing.UpdatedAt) > s.ttl {
			delete(s.stats, id)
		}
	}
	s.mu.Unlock()
}

// Get returns the stats for nodeID. Returns false if the entry is absent or expired.
func (s *NodeStatsStore) Get(nodeID string) (NodeStats, bool) {
	s.mu.RLock()
	ns, ok := s.stats[nodeID]
	s.mu.RUnlock()
	if !ok || time.Since(ns.UpdatedAt) > s.ttl {
		return NodeStats{}, false
	}
	return ns, true
}

// GetAll returns all non-expired entries.
func (s *NodeStatsStore) GetAll() []NodeStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	now := time.Now()
	out := make([]NodeStats, 0, len(s.stats))
	for _, ns := range s.stats {
		if now.Sub(ns.UpdatedAt) <= s.ttl {
			out = append(out, ns)
		}
	}
	return out
}

// UpdateDiskStats atomically updates only the disk fields for nodeID, preserving all other fields.
// No-op if nodeID is not in the store. Clamps usedPct to [0, 100].
func (s *NodeStatsStore) UpdateDiskStats(nodeID string, usedPct float64, availBytes uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ns, ok := s.stats[nodeID]
	if !ok {
		return
	}
	if usedPct < 0 {
		usedPct = 0
	} else if usedPct > 100 {
		usedPct = 100
	}
	ns.DiskUsedPct = usedPct
	ns.DiskAvailBytes = availBytes
	ns.UpdatedAt = time.Now()
	s.stats[nodeID] = ns
}

// Len returns the count of non-expired entries.
func (s *NodeStatsStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	now := time.Now()
	count := 0
	for _, ns := range s.stats {
		if now.Sub(ns.UpdatedAt) <= s.ttl {
			count++
		}
	}
	return count
}
