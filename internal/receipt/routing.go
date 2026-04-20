package receipt

import "sync"

// RoutingCache tracks which peers recently produced which HealReceipts, based
// on gossip messages each peer broadcasts (rolling window of the last N
// receipt IDs). Dashboard queries for a specific receipt_id hit Lookup first
// and only fall back to a cluster broadcast when Lookup misses.
//
// Storage is map[nodeID][]receiptID. The rolling window means each Update
// from a given node replaces that node's list wholesale — older IDs drop
// out of the cache naturally without explicit eviction logic.
//
// Concurrency: sync.RWMutex — Lookup is the hot path (every dashboard
// request), Update fires once per gossip tick per peer. An atomic-pointer
// copy-on-write approach is tracked in TODOS.md (Phase 17 lock-free) but
// is premature before we measure real Lookup throughput.
type RoutingCache struct {
	mu    sync.RWMutex
	peers map[string][]string // nodeID → ordered receipt IDs
}

// NewRoutingCache returns an empty cache.
func NewRoutingCache() *RoutingCache {
	return &RoutingCache{peers: make(map[string][]string)}
}

// Update replaces the list of receipt IDs associated with nodeID. The caller
// is expected to pass a freshly-allocated slice (the cache holds onto the
// reference without copying). Passing an empty slice clears the node's
// entries while keeping it in the map.
func (c *RoutingCache) Update(nodeID string, ids []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.peers[nodeID] = ids
}

// Lookup returns the nodeID known to hold receiptID and true, or "", false
// when no peer has reported it within their rolling window.
//
// Scan cost is O(peers × window_size). For the Slice 2 target of ~10 peers
// × 50 IDs = 500 comparisons per lookup, this stays under a microsecond.
// If peer count grows, switch to an inverted id→nodeID map (see TODOS.md
// lock-free refactor).
func (c *RoutingCache) Lookup(receiptID string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for nodeID, ids := range c.peers {
		for _, id := range ids {
			if id == receiptID {
				return nodeID, true
			}
		}
	}
	return "", false
}

// Evict removes nodeID and its receipt IDs. Evicting an unknown node is a
// no-op. Callers use this when a peer leaves the cluster.
func (c *RoutingCache) Evict(nodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.peers, nodeID)
}
