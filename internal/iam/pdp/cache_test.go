package pdp

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDecisionCache_FreshWithinTTL(t *testing.T) {
	c := newDecisionCache(64)
	base := time.Unix(1000, 0)
	c.Put("k", "allow", "ok", 10*time.Second, base)

	ent, st := c.Lookup("k", time.Hour, base.Add(5*time.Second)) // ttl/2
	require.Equal(t, cacheFresh, st)
	require.Equal(t, "allow", ent.decision)
	require.Equal(t, "ok", ent.reason)
}

func TestDecisionCache_StaleIsPreserved(t *testing.T) {
	c := newDecisionCache(64)
	base := time.Unix(1000, 0)
	c.Put("k", "allow", "ok", 10*time.Second, base)

	// age = ttl+1s, within a large graceTTL.
	staleNow := base.Add(11 * time.Second)
	ent, st := c.Lookup("k", time.Hour, staleNow)
	require.Equal(t, cacheStale, st)
	require.Equal(t, "allow", ent.decision)
	require.Equal(t, 1, c.Len(), "stale read must not evict")

	// A SECOND stale read must still find it: the first stale read did not evict
	// it and did not reset its age (age is since decidedAt, not since read).
	ent2, st2 := c.Lookup("k", time.Hour, staleNow.Add(time.Second))
	require.Equal(t, cacheStale, st2)
	require.Equal(t, "allow", ent2.decision)
	require.Equal(t, 1, c.Len())
}

func TestDecisionCache_BeyondGraceEvicts(t *testing.T) {
	c := newDecisionCache(64)
	base := time.Unix(1000, 0)
	c.Put("k", "allow", "ok", 10*time.Second, base)

	// age past graceTTL.
	_, st := c.Lookup("k", 20*time.Second, base.Add(21*time.Second))
	require.Equal(t, cacheMiss, st)
	require.Equal(t, 0, c.Len(), "beyond-grace lookup must evict")
}

func TestDecisionCache_ZeroGraceEvictsWhenStale(t *testing.T) {
	c := newDecisionCache(64)
	base := time.Unix(1000, 0)
	c.Put("k", "allow", "ok", 10*time.Second, base)

	_, st := c.Lookup("k", 0, base.Add(11*time.Second)) // age>ttl, graceTTL==0
	require.Equal(t, cacheMiss, st)
	require.Equal(t, 0, c.Len())
}

func TestDecisionCache_DenyCached(t *testing.T) {
	c := newDecisionCache(64)
	base := time.Unix(1000, 0)
	c.Put("d", "deny", "nope", 30*time.Second, base)

	ent, st := c.Lookup("d", time.Hour, base.Add(5*time.Second))
	require.Equal(t, cacheFresh, st)
	require.Equal(t, "deny", ent.decision)
	require.Equal(t, "nope", ent.reason)
}

func TestDecisionCache_LRUEviction(t *testing.T) {
	c := newDecisionCache(64) // perShard = ceil(64/16) = 4 => cap 64
	total := c.perShard * numShards
	base := time.Unix(1000, 0)

	// Insert more than capacity distinct keys.
	n := total + 100
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		keys[i] = fmt.Sprintf("key-%d", i)
		c.Put(keys[i], "allow", "ok", time.Hour, base)
	}

	require.LessOrEqual(t, c.Len(), total, "Len must stay within perShard*numShards")

	// An early key that was never re-touched should have been evicted. Find the
	// shard for keys[0] and confirm that shard's oldest entries were pushed out.
	// Because eviction is per-shard LRU, the first key inserted into its shard is
	// gone once that shard overflows. With n far exceeding cap, keys[0] is gone.
	_, st := c.Lookup(keys[0], 0, base.Add(time.Millisecond))
	require.Equal(t, cacheMiss, st, "early untouched key should be evicted")
}

func TestDecisionCache_PutReplaceResetsDecidedAt(t *testing.T) {
	c := newDecisionCache(64)
	base := time.Unix(1000, 0)

	// First put: ttl 10s at base.
	c.Put("k", "deny", "old", 10*time.Second, base)

	// Re-put at base+8s with a NEW decision and a fresh 10s ttl.
	replaceAt := base.Add(8 * time.Second)
	c.Put("k", "allow", "new", 10*time.Second, replaceAt)

	// now = base+15s: past the OLD ttl window (base+10s) but within the NEW one
	// (replaceAt+10s = base+18s). Must be fresh with the updated decision.
	ent, st := c.Lookup("k", time.Hour, base.Add(15*time.Second))
	require.Equal(t, cacheFresh, st, "decidedAt must reset to the re-put time")
	require.Equal(t, "allow", ent.decision)
	require.Equal(t, "new", ent.reason)
}

func TestDecisionCache_Clear(t *testing.T) {
	c := newDecisionCache(64)
	base := time.Unix(1000, 0)
	for i := 0; i < 10; i++ {
		c.Put(fmt.Sprintf("k-%d", i), "allow", "ok", time.Hour, base)
	}
	require.Equal(t, 10, c.Len())
	c.Clear()
	require.Equal(t, 0, c.Len())
}

func TestDecisionCache_CapacityRoundsUp(t *testing.T) {
	// maxEntries=10 => perShard = ceil(10/16) = 1 => total cap 16 (rounds UP).
	c := newDecisionCache(10)
	require.Equal(t, 1, c.perShard)
	base := time.Unix(1000, 0)
	for i := 0; i < 100; i++ {
		c.Put(fmt.Sprintf("k-%d", i), "allow", "ok", time.Hour, base)
	}
	require.LessOrEqual(t, c.Len(), c.perShard*numShards)
}

func TestDecisionCache_Concurrent(t *testing.T) {
	c := newDecisionCache(256)
	base := time.Unix(1000, 0)
	const goroutines = 50
	const ops = 200

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				// Mix of shared keys (contention) and distinct keys.
				shared := fmt.Sprintf("shared-%d", i%8)
				distinct := fmt.Sprintf("g%d-%d", g, i)
				now := base.Add(time.Duration(i) * time.Millisecond)
				c.Put(shared, "allow", "ok", time.Second, now)
				c.Put(distinct, "deny", "no", time.Second, now)
				c.Lookup(shared, time.Minute, now)
				c.Lookup(distinct, time.Minute, now)
				c.Lookup("absent", time.Minute, now)
			}
		}(g)
	}
	wg.Wait()
	// No assertion on exact size (LRU + sharding makes it nondeterministic); the
	// point is the -race detector stays clean and nothing panics.
	require.LessOrEqual(t, c.Len(), c.perShard*numShards)
}
