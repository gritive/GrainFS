package receipt

import (
	"sync"
	"sync/atomic"
)

// RoutingCache tracks which peers recently produced which HealReceipts, based
// on gossip messages each peer broadcasts (rolling window of the last N
// receipt IDs). Dashboard queries for a specific receipt_id hit Lookup first
// and only fall back to a cluster broadcast when Lookup misses.
//
// Storage is map[nodeID][]receiptID. The rolling window means each Update
// from a given node replaces that node's list wholesale — older IDs drop
// out of the cache naturally without explicit eviction logic.
//
// Concurrency: Lookup is the hot path (every dashboard request) and runs
// lock-free over an immutable atomic snapshot. Update/Evict are rare gossip
// events, so they serialize with a writer mutex, copy the small rolling window,
// and publish the next snapshot with a single atomic Store.
type RoutingCache struct {
	writeMu sync.Mutex
	snap    atomic.Pointer[routingSnapshot]
}

type routingSnapshot struct {
	peers     map[string][]string // nodeID → ordered receipt IDs
	byReceipt map[string]string   // receiptID → nodeID
}

// NewRoutingCache returns an empty cache.
func NewRoutingCache() *RoutingCache {
	c := &RoutingCache{}
	c.snap.Store(&routingSnapshot{
		peers:     make(map[string][]string),
		byReceipt: make(map[string]string),
	})
	return c
}

// Update replaces the list of receipt IDs associated with nodeID. The caller
// may reuse or mutate ids after Update returns; the cache copies the slice into
// an immutable snapshot. Passing an empty slice clears the node's entries while
// keeping it in the map.
func (c *RoutingCache) Update(nodeID string, ids []string) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	cur := c.snap.Load()
	next := cur.clone()
	if prev := next.peers[nodeID]; len(prev) > 0 {
		for _, id := range prev {
			if next.byReceipt[id] == nodeID {
				delete(next.byReceipt, id)
			}
		}
	}
	copied := append([]string(nil), ids...)
	next.peers[nodeID] = copied
	for _, id := range copied {
		next.byReceipt[id] = nodeID
	}
	c.snap.Store(next)
}

// Lookup returns the nodeID known to hold receiptID and true, or "", false
// when no peer has reported it within their rolling window.
//
// Lookup is O(1), lock-free, and allocation-free.
func (c *RoutingCache) Lookup(receiptID string) (string, bool) {
	snap := c.snap.Load()
	nodeID, ok := snap.byReceipt[receiptID]
	return nodeID, ok
}

// Evict removes nodeID and its receipt IDs. Evicting an unknown node is a
// no-op. Callers use this when a peer leaves the cluster.
func (c *RoutingCache) Evict(nodeID string) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	cur := c.snap.Load()
	next := cur.clone()
	if prev := next.peers[nodeID]; len(prev) > 0 {
		for _, id := range prev {
			if next.byReceipt[id] == nodeID {
				delete(next.byReceipt, id)
			}
		}
	}
	delete(next.peers, nodeID)
	c.snap.Store(next)
}

func (s *routingSnapshot) clone() *routingSnapshot {
	next := &routingSnapshot{
		peers:     make(map[string][]string, len(s.peers)),
		byReceipt: make(map[string]string, len(s.byReceipt)),
	}
	for nodeID, ids := range s.peers {
		next.peers[nodeID] = ids
	}
	for receiptID, nodeID := range s.byReceipt {
		next.byReceipt[receiptID] = nodeID
	}
	return next
}
