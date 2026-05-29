package pdp

import (
	"container/list"
	"hash/fnv"
	"sync"
	"time"
)

// numShards is the fixed shard count of the decision cache. The shard for a key
// is FNV-1a(key) % numShards.
//
// A lock-free LRU was considered but rejected: LRU eviction needs coordinated
// ordering (move-to-front + back-eviction touch the same list), which is awkward
// to make correct without a lock. Sharding is the contention mitigation instead:
// each shard carries its own mutex, so unrelated keys rarely contend.
const numShards = 16

// cacheState is the outcome of a cache Lookup.
type cacheState int

const (
	cacheMiss cacheState = iota
	cacheFresh
	cacheStale
)

// cacheEntry is one cached PDP decision. age is computed as now-decidedAt and is
// NEVER reset on access: age tracks time since the PDP decision, not since last
// read, so a fresh read does not extend the entry's life.
type cacheEntry struct {
	key       string
	decision  string
	reason    string
	decidedAt time.Time
	ttl       time.Duration
}

// cacheShard is one of the numShards independently-locked LRU partitions.
type cacheShard struct {
	mu sync.Mutex
	m  map[string]*list.Element
	ll *list.List // front = most-recently-used
}

// decisionCache is a sharded TTL+LRU cache of PDP decisions with a
// stale-preserving lookup: an entry past its ttl but within graceTTL is returned
// as cacheStale WITHOUT being evicted or moved-to-front, so the decorator can
// grace-serve it after a PDP failure while a fresh entry is fetched.
type decisionCache struct {
	shards   [numShards]cacheShard
	perShard int
}

// newDecisionCache returns a cache with numShards shards. The per-shard cap is
// ceil(maxEntries/numShards) (min 1), so the total capacity (perShard*numShards)
// may round UP above maxEntries; that rounding is the documented invariant.
func newDecisionCache(maxEntries int) *decisionCache {
	perShard := (maxEntries + numShards - 1) / numShards
	if perShard < 1 {
		perShard = 1
	}
	c := &decisionCache{perShard: perShard}
	for i := range c.shards {
		c.shards[i].m = make(map[string]*list.Element)
		c.shards[i].ll = list.New()
	}
	return c
}

// shardFor returns the shard for key via FNV-1a(key) % numShards.
func (c *decisionCache) shardFor(key string) *cacheShard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return &c.shards[h.Sum32()%numShards]
}

// Lookup classifies the entry for key against ttl/graceTTL using the injected
// now. Outcomes:
//   - absent                              -> cacheMiss
//   - age <= ttl                          -> cacheFresh (move-to-front)
//   - ttl < age <= graceTTL (graceTTL>0)  -> cacheStale (NO evict, NO move-to-front)
//   - otherwise                           -> cacheMiss (evict)
//
// age is now-decidedAt and is never reset; a stale read leaves the entry exactly
// as it was so a later lookup still sees it.
func (c *decisionCache) Lookup(key string, graceTTL time.Duration, now time.Time) (cacheEntry, cacheState) {
	sh := c.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	el, ok := sh.m[key]
	if !ok {
		return cacheEntry{}, cacheMiss
	}
	ent := el.Value.(*cacheEntry)
	age := now.Sub(ent.decidedAt)

	if age <= ent.ttl {
		sh.ll.MoveToFront(el)
		return *ent, cacheFresh
	}
	if graceTTL > 0 && age <= graceTTL {
		// Stale-preserving: return a copy but do NOT touch LRU order or evict, so
		// the entry survives this read for a subsequent grace-serve.
		return *ent, cacheStale
	}
	// Beyond grace (or no grace): evict.
	sh.ll.Remove(el)
	delete(sh.m, key)
	return cacheEntry{}, cacheMiss
}

// Put upserts the decision for key. If present, it updates the value
// (decision/reason/decidedAt/ttl) and moves the entry to the front; otherwise it
// inserts at the front. After insert it evicts from the back (oldest) until the
// shard is within its per-shard cap. ttl<=0 is treated as a defensive no-op.
func (c *decisionCache) Put(key, decision, reason string, ttl time.Duration, now time.Time) {
	if ttl <= 0 {
		return
	}
	sh := c.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	if el, ok := sh.m[key]; ok {
		ent := el.Value.(*cacheEntry)
		ent.decision = decision
		ent.reason = reason
		ent.decidedAt = now
		ent.ttl = ttl
		sh.ll.MoveToFront(el)
		return
	}

	ent := &cacheEntry{key: key, decision: decision, reason: reason, decidedAt: now, ttl: ttl}
	sh.m[key] = sh.ll.PushFront(ent)

	for sh.ll.Len() > c.perShard {
		back := sh.ll.Back()
		if back == nil {
			break
		}
		old := back.Value.(*cacheEntry)
		sh.ll.Remove(back)
		delete(sh.m, old.key)
	}
}

// Len returns the total number of entries across all shards.
func (c *decisionCache) Len() int {
	n := 0
	for i := range c.shards {
		sh := &c.shards[i]
		sh.mu.Lock()
		n += sh.ll.Len()
		sh.mu.Unlock()
	}
	return n
}

// Clear empties every shard.
func (c *decisionCache) Clear() {
	for i := range c.shards {
		sh := &c.shards[i]
		sh.mu.Lock()
		sh.m = make(map[string]*list.Element)
		sh.ll.Init()
		sh.mu.Unlock()
	}
}
