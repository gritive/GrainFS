package shardcache

import (
	"fmt"
	"hash/fnv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGet_DisabledReturnsMiss(t *testing.T) {
	c := New(0)
	c.Put("a", []byte("data"))
	if _, ok := c.Get("a"); ok {
		require.Fail(t, "disabled cache must always miss")
	}
	s := c.Stats()
	require.Zero(t, s.Hits, "disabled cache should not record hits")
	require.Zero(t, s.Misses, "disabled cache should not record misses")
}

func TestPutGet_RoundTrip(t *testing.T) {
	c := New(1024)
	c.Put("bucket/key/v1/0", []byte("shard-0-bytes"))
	got, ok := c.Get("bucket/key/v1/0")
	require.True(t, ok, "expected hit")
	require.Equal(t, "shard-0-bytes", string(got))
}

func TestPeek_DoesNotRecordHitOrMiss(t *testing.T) {
	c := New(1024)
	c.Put("bucket/key/v1/0", []byte("shard-0-bytes"))

	got, ok := c.Peek("bucket/key/v1/0")
	require.True(t, ok)
	require.Equal(t, "shard-0-bytes", string(got))
	_, ok = c.Peek("bucket/key/v1/1")
	require.False(t, ok)

	stats := c.Stats()
	require.Zero(t, stats.Hits)
	require.Zero(t, stats.Misses)
}

func TestPut_CopiesPayload(t *testing.T) {
	// getObjectEC reuses no shard buffers, but the cache contract is
	// "we own the bytes" so callers can mutate the source after Put.
	c := New(1024)
	src := []byte("original")
	c.Put("k", src)
	src[0] = 'X'
	got, _ := c.Get("k")
	require.Equal(t, "original", string(got), "cache must not alias caller buffer")
}

func shardIndex(key string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() & (numShards - 1))
}

// keysOnSameShard finds N keys hashing to the same shard. Eviction
// happens per-shard so eviction tests need same-shard keys.
func keysOnSameShard(t *testing.T, n int) []string {
	t.Helper()
	target := shardIndex("seed")
	keys := []string{"seed"}
	for i := 0; len(keys) < n && i < 100000; i++ {
		k := fmt.Sprintf("k-%06d", i)
		if shardIndex(k) == target {
			keys = append(keys, k)
		}
	}
	require.GreaterOrEqualf(t, len(keys), n, "only found %d collisions in 100k tries; need %d", len(keys), n)
	return keys
}

func TestPut_LRUEvicts(t *testing.T) {
	c := New(numShards * 8)
	keys := keysOnSameShard(t, 3)
	c.Put(keys[0], []byte("AAAA"))
	c.Put(keys[1], []byte("BBBB"))
	c.Put(keys[2], []byte("CCCC"))
	if _, ok := c.Get(keys[0]); ok {
		require.Failf(t, "entry should have been evicted", "%s should have been evicted", keys[0])
	}
	if _, ok := c.Get(keys[1]); !ok {
		require.Failf(t, "entry should still be resident", "%s should still be resident", keys[1])
	}
	if _, ok := c.Get(keys[2]); !ok {
		require.Failf(t, "entry should still be resident", "%s should still be resident", keys[2])
	}
	s := c.Stats()
	require.Equal(t, uint64(1), s.Evictions)
}

func TestPut_RefreshOnRepeat(t *testing.T) {
	c := New(numShards * 8)
	keys := keysOnSameShard(t, 3)
	c.Put(keys[0], []byte("AAAA"))
	c.Put(keys[1], []byte("BBBB"))
	c.Put(keys[0], []byte("ZZZZ"))
	got, _ := c.Get(keys[0])
	require.Equal(t, "AAAA", string(got), "re-Put should not replace data without Invalidate")
	c.Put(keys[2], []byte("CCCC"))
	if _, ok := c.Get(keys[1]); ok {
		require.Failf(t, "entry should have been evicted", "%s should have been evicted (keys[0] refreshed via re-Put)", keys[1])
	}
}

func TestPut_OversizedPayloadSkipped(t *testing.T) {
	c := New(numShards * 8)
	keys := keysOnSameShard(t, 2)
	c.Put(keys[0], []byte("AAAA"))
	c.Put(keys[1], []byte("payload-too-large-to-fit-in-8-bytes"))
	if _, ok := c.Get(keys[0]); !ok {
		require.Fail(t, "oversized Put evicted unrelated entry")
	}
	if _, ok := c.Get(keys[1]); ok {
		require.Fail(t, "oversized payload should not be cached")
	}
}

func TestInvalidate_RemovesEntry(t *testing.T) {
	c := New(64)
	c.Put("a", []byte("AAAA"))
	c.Invalidate("a")
	if _, ok := c.Get("a"); ok {
		require.Fail(t, "Invalidate did not remove entry")
	}
	require.Zero(t, c.Stats().ResidentByte, "resident bytes should be 0 after invalidate")
}

func TestInvalidate_NoOpOnMissing(t *testing.T) {
	c := New(64)
	c.Invalidate("nope")
}

func TestConcurrent_GetPutInvalidate(t *testing.T) {
	c := New(1024 * 1024)
	const goroutines = 8
	const iters = 5000
	var wg sync.WaitGroup
	wg.Add(goroutines * 3)
	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				c.Put(fmt.Sprintf("k%d-%d", g, i%64), []byte("payload"))
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				c.Get(fmt.Sprintf("k%d-%d", g, i%64))
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				c.Invalidate(fmt.Sprintf("k%d-%d", g, i%64))
			}
		}()
	}
	wg.Wait()
	s := c.Stats()
	require.GreaterOrEqual(t, s.ResidentByte, int64(0))
	require.LessOrEqual(t, s.ResidentByte, s.CapacityByte)
}
