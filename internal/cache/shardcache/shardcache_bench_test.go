package shardcache

import (
	"fmt"
	"sync/atomic"
	"testing"
)

// BenchmarkGetHitParallel measures the read hot path under concurrency: a warm
// working set with ~95% Get hits and occasional Put (miss refill). This is the
// EC reconstruction access pattern. It exists to decide whether the
// sharded-mutex Get/Peek path is worth replacing with a lock-free read path.
func BenchmarkGetHitParallel(b *testing.B) {
	const ws = 512
	c := New(int64(ws) * 4096 * 2) // budget holds the whole working set warm
	payload := make([]byte, 4096)
	keys := make([]string, ws)
	for i := range keys {
		keys[i] = fmt.Sprintf("bucket/object/shard-%d", i)
		c.Put(keys[i], payload)
	}
	var ctr atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := ctr.Add(1)
			k := keys[i&(ws-1)]
			if _, ok := c.Get(k); !ok {
				c.Put(k, payload)
			}
		}
	})
}

// BenchmarkGetMissPutParallel models the cold large-set regime: working set far
// exceeds capacity so nearly every Get misses and triggers a Put+evict. This is
// where Put-side locking and the make+copy churn dominate.
func BenchmarkGetMissPutParallel(b *testing.B) {
	const ws = 8192
	c := New(64 * 4096) // tiny budget vs working set => ~all miss+evict
	payload := make([]byte, 4096)
	keys := make([]string, ws)
	for i := range keys {
		keys[i] = fmt.Sprintf("bucket/object/shard-%d", i)
	}
	var ctr atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := ctr.Add(1)
			k := keys[i&(ws-1)]
			if _, ok := c.Get(k); !ok {
				c.Put(k, payload)
			}
		}
	})
}
