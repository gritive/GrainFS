// Package shardcache is a bounded in-memory cache for EC shards.
//
// EC shard cache (Phase 2 #3 follow-up). Multi-node E2E measurement on
// PR #71's --measure-read-amp baseline showed large_repeat 16MB×10 hits
// 90% at every reachable cache size — CachedBackend's 4 MB-per-object
// cap forces every GET to bypass and re-run getObjectEC, which fans out
// K shard reads. The same shards come back every iteration.
//
// This cache sits in front of getObjectEC's per-shard fan-out. A full
// cache hit (every needed shard cached) skips disk/network entirely
// and the call returns from ECReconstruct alone. Partial hit launches
// goroutines only for the missing indices.
//
// Sizing: capacity is in bytes (not entries). Default 256 MB matches
// the readamp simulator point where the curve saturates. Shards are
// fractions of object size — for a 16 MB object at k=2, each shard is
// ≈8 MB, so 256 MB holds dozens of recent reads. Set to 0 to disable.
//
// Concurrency: same sharded-LRU shape as blockcache. 16 shards keyed
// by FNV-1a(key). Each shard owns a tiny LRU + sync.Mutex; contention
// scales with concurrent accesses on the SAME shard, not process-wide.
// We considered an actor-goroutine pattern (one owner, channel-based
// requests) per the project's lock-free preference, but rejected it
// for the same reason as blockcache: every getObjectEC fan-out would
// pay a channel send/recv round-trip (~hundreds of ns) where a brief
// sharded mutex acquire is ~tens of ns. The DegradedTracker actor
// pattern fits low-frequency state machines; this is a high-frequency
// read primitive on the EC reconstruction hot path. Sharded mutex is
// the right tool here.
//
// Counters use atomic.Uint64/Int64 so Stats() never contends with
// hot-path Get/Put.
package shardcache

import (
	"container/list"
	"hash/fnv"
	"sync"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// numShards is fixed at 16. Power of two so the modulus folds to a
// bitwise AND. Matches blockcache for symmetry.
const numShards = 16

// Process-wide Prometheus instruments. At most one EC shard cache per
// process, so package globals beat per-instance metric registration.
var (
	mHits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_ec_shard_cache_hits_total",
		Help: "EC shard cache hits since process start.",
	})
	mMisses = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_ec_shard_cache_misses_total",
		Help: "EC shard cache misses (caller fell through to ReadShard / ReadLocalShard).",
	})
	mEvictions = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_ec_shard_cache_evictions_total",
		Help: "EC shard cache LRU evictions.",
	})
	mResident = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "grainfs_ec_shard_cache_resident_bytes",
		Help: "Bytes currently held by the EC shard cache (sum across shards).",
	})
	mCapacity = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "grainfs_ec_shard_cache_capacity_bytes",
		Help: "Configured byte capacity of the EC shard cache.",
	})
)

type entry struct {
	key  string
	data []byte
}

// shard is one slice of the cache. Each owns its own LRU and byte
// budget. capacity = global capacity / numShards.
type shard struct {
	capacityBytes int64

	mu      sync.Mutex
	entries map[string]*list.Element
	lru     *list.List
	bytes   int64
}

// Cache is a sharded LRU keyed by string, bounded by total bytes.
type Cache struct {
	shards        [numShards]*shard
	capacityBytes int64

	hits      atomic.Uint64
	misses    atomic.Uint64
	evictions atomic.Uint64
	residency atomic.Int64
}

// New builds a cache with the given byte budget. capacity <= 0 means
// disabled: every Get returns miss, Put is a no-op. Returns non-nil so
// callers stay branch-free.
func New(capacityBytes int64) *Cache {
	c := &Cache{capacityBytes: capacityBytes}
	perShard := capacityBytes / numShards
	if capacityBytes > 0 && perShard < 1 {
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
		return c.shards[0]
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return c.shards[h.Sum32()&(numShards-1)]
}

// Get returns (data, true) on hit, (nil, false) on miss. Returned
// slice is the cached buffer — callers MUST treat as read-only.
// getObjectEC immediately passes shards[i] into ECReconstruct, which
// only reads, so this is safe.
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

// Put inserts a copy of data under key. On duplicate key, refreshes
// LRU position but does NOT replace bytes — write paths must
// Invalidate first to land fresh content.
func (c *Cache) Put(key string, data []byte) {
	if c.capacityBytes <= 0 {
		return
	}
	s := c.shardFor(key)
	if int64(len(data)) > s.capacityBytes {
		// One payload bigger than the per-shard budget. Skip caching
		// rather than evict the whole shard to make room.
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

// Invalidate drops the entry for key if present. Write paths
// (PutObject overwrite, DeleteObject, repairShardEC) call this so
// later reads see post-write state.
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

// Stats is a snapshot of cache state.
type Stats struct {
	Hits         uint64
	Misses       uint64
	Evictions    uint64
	ResidentByte int64
	CapacityByte int64
}

// Stats returns counters via atomic loads — never contends with
// hot-path Get/Put.
func (c *Cache) Stats() Stats {
	return Stats{
		Hits:         c.hits.Load(),
		Misses:       c.misses.Load(),
		Evictions:    c.evictions.Load(),
		ResidentByte: c.residency.Load(),
		CapacityByte: c.capacityBytes,
	}
}
