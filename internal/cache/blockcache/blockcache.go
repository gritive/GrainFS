// Package blockcache is a bounded in-memory cache for volume blocks.
//
// Phase 2 #3 narrow scope: every volume.Manager.ReadAt block fetch
// consults this cache before going to the backend. Hits skip the
// backend GetObject + Badger lookup entirely; misses populate the
// cache so subsequent reads of the same physical key are free.
//
// The cache is intentionally narrow — only volume-block reads pass
// through. Object-layer reads keep going through CachedBackend
// (`internal/storage/cache.go`), which simulator measurements showed
// already absorbs object-layer locality. Stacking another cache there
// would be double-caching with no marginal value.
//
// Sizing: capacity is in bytes (not entries) so callers express a
// memory budget directly. Default 64 MB matches the readamp simulator
// "knee" — workloads with locality saturate around that budget. Set
// to zero to disable; serve.go does that when --block-cache-size=0.
//
// Concurrency: sharded LRU. The cache is split into N independent
// shards keyed by FNV-1a(key) % N. Each shard has its own LRU + tiny
// mutex; contention scales with concurrent accesses on the SAME shard
// rather than process-wide. We considered an actor-goroutine pattern
// (one owner goroutine, channel-based requests) per the project rule
// to prefer actor/lock-free over plain mutex, but rejected it for
// this hot path: every volume.ReadAt block fetch would pay a channel
// send/recv round-trip (~hundreds of ns) where a brief sharded
// mutex acquire is ~tens of ns. The DegradedTracker actor pattern
// fits low-frequency state machines; this cache is a high-frequency
// read primitive. Sharded mutex is the right tool here.
//
// Counters use atomic.Uint64 directly (lock-free) so Stats() reads do
// not contend with hot-path Get/Put. Resident byte count is per-shard
// atomic to keep the same property when summing.
package blockcache

import (
	"container/list"
	"hash/fnv"
	"sync"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// numShards is fixed at 16 — enough to break a busy NBD workload's
// per-shard contention, small enough that per-shard memory budgets
// stay coarse-grained. Picked as a power of two so the modulus
// becomes a bitwise AND.
const numShards = 16

// Process-wide Prometheus instruments. We have at most one block
// cache per process, so package globals beat per-instance metric
// registration races. promauto registers on first import, never on
// subsequent New() calls.
var (
	mHits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_block_cache_hits_total",
		Help: "Volume block cache hits since process start.",
	})
	mMisses = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_block_cache_misses_total",
		Help: "Volume block cache misses (caller fell through to backend GetObject).",
	})
	mEvictions = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_block_cache_evictions_total",
		Help: "Volume block cache LRU evictions.",
	})
	mResident = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "grainfs_block_cache_resident_bytes",
		Help: "Bytes currently held by the volume block cache (sum across shards).",
	})
	mCapacity = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "grainfs_block_cache_capacity_bytes",
		Help: "Configured byte capacity of the volume block cache.",
	})
)

type entry struct {
	key  string
	data []byte
}

// shard is one slice of the cache. Each shard owns its own LRU and
// byte-budget. capacity is the per-shard budget — global capacity
// divided across shards at construction.
type shard struct {
	capacityBytes int64

	mu      sync.Mutex
	entries map[string]*list.Element
	lru     *list.List
	bytes   int64
}

// Cache is a sharded LRU of byte payloads keyed by string.
type Cache struct {
	shards        [numShards]*shard
	capacityBytes int64

	hits      atomic.Uint64
	misses    atomic.Uint64
	evictions atomic.Uint64
	residency atomic.Int64
}

// New builds a cache with the given total byte capacity. capacity <=
// 0 means a disabled cache: every Get returns miss, Put is a no-op.
// We still return a non-nil *Cache so callers stay branch-free.
func New(capacityBytes int64) *Cache {
	c := &Cache{capacityBytes: capacityBytes}
	perShard := capacityBytes / numShards
	if capacityBytes > 0 && perShard < 1 {
		// Pathologically small budget; fall back to one shard
		// holding the whole budget.
		perShard = capacityBytes
	}
	for i := range c.shards {
		c.shards[i] = &shard{
			capacityBytes: perShard,
			entries:       make(map[string]*list.Element),
			lru:           list.New(),
		}
	}
	mCapacity.Set(float64(capacityBytes))
	return c
}

func (c *Cache) shardFor(key string) *shard {
	if c.capacityBytes <= 0 {
		// Disabled cache — any shard works, none of them will accept
		// inserts. We still return shards[0] so Get can lock-free
		// short-circuit before reaching the shard.
		return c.shards[0]
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return c.shards[h.Sum32()&(numShards-1)]
}

// Get returns (data, true) on hit, (nil, false) on miss. The returned
// slice is the cached buffer — callers MUST treat it as read-only and
// copy if they need to mutate. volume.ReadAt copies into the user's
// output buffer immediately, which is safe.
func (c *Cache) Get(key string) ([]byte, bool) {
	if c.capacityBytes <= 0 {
		return nil, false
	}
	s := c.shardFor(key)
	s.mu.Lock()
	elem, ok := s.entries[key]
	if !ok {
		s.mu.Unlock()
		c.misses.Add(1)
		mMisses.Inc()
		return nil, false
	}
	s.lru.MoveToFront(elem)
	data := elem.Value.(*entry).data
	s.mu.Unlock()
	c.hits.Add(1)
	mHits.Inc()
	return data, true
}

// Put inserts a copy of data under key. If the key already exists,
// the existing entry is refreshed (moved to MRU) but its data is NOT
// replaced — on a write path the caller calls Invalidate first.
//
// Memory: Put copies data so the cache owns its bytes. Callers that
// share buffers do not have to worry about later mutating the source.
func (c *Cache) Put(key string, data []byte) {
	if c.capacityBytes <= 0 {
		return
	}
	s := c.shardFor(key)
	if int64(len(data)) > s.capacityBytes {
		// Pathological: single payload bigger than per-shard capacity.
		// Skip caching rather than evict everything to make room.
		return
	}
	cp := make([]byte, len(data))
	copy(cp, data)

	s.mu.Lock()
	defer s.mu.Unlock()

	if elem, ok := s.entries[key]; ok {
		s.lru.MoveToFront(elem)
		return
	}
	for s.bytes+int64(len(cp)) > s.capacityBytes && s.lru.Len() > 0 {
		back := s.lru.Back()
		bEntry := back.Value.(*entry)
		s.lru.Remove(back)
		delete(s.entries, bEntry.key)
		s.bytes -= int64(len(bEntry.data))
		c.residency.Add(-int64(len(bEntry.data)))
		c.evictions.Add(1)
		mEvictions.Inc()
	}
	e := &entry{key: key, data: cp}
	elem := s.lru.PushFront(e)
	s.entries[key] = elem
	s.bytes += int64(len(cp))
	c.residency.Add(int64(len(cp)))
	mResident.Set(float64(c.residency.Load()))
}

// Invalidate drops the entry for key if present. The volume write
// path calls this for every block touched so a subsequent read sees
// the post-write state.
func (c *Cache) Invalidate(key string) {
	if c.capacityBytes <= 0 {
		return
	}
	s := c.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	if elem, ok := s.entries[key]; ok {
		e := elem.Value.(*entry)
		s.lru.Remove(elem)
		delete(s.entries, key)
		s.bytes -= int64(len(e.data))
		c.residency.Add(-int64(len(e.data)))
		mResident.Set(float64(c.residency.Load()))
	}
}

// Stats reports current counters and resident byte count.
type Stats struct {
	Hits         uint64
	Misses       uint64
	Evictions    uint64
	ResidentByte int64
	CapacityByte int64
}

// Stats returns a snapshot of cache state. All fields are read via
// atomic loads so Stats does not contend with hot-path Get/Put.
func (c *Cache) Stats() Stats {
	return Stats{
		Hits:         c.hits.Load(),
		Misses:       c.misses.Load(),
		Evictions:    c.evictions.Load(),
		ResidentByte: c.residency.Load(),
		CapacityByte: c.capacityBytes,
	}
}
