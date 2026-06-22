package cluster

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// liveLen reports the total number of currently-held keys — proof that the map
// stays bounded (returns to zero once every holder releases). Test-only.
func (k *keyedRWMutex) liveLen() int {
	n := 0
	for i := range k.shards {
		sh := &k.shards[i]
		sh.mu.Lock()
		n += len(sh.m)
		sh.mu.Unlock()
	}
	return n
}

func TestKeyedRWMutex_SameKeySameLock_DistinctKeysDiffer(t *testing.T) {
	var k keyedRWMutex

	// Same key, both holders live → identical underlying RWMutex (mutual exclusion).
	m1, r1 := k.acquire("bkt\x00k")
	m2, r2 := k.acquire("bkt\x00k")
	require.Same(t, m1, m2, "same key must converge on one RWMutex while held")

	// Different key → different lock (no false serialization).
	m3, r3 := k.acquire("bkt\x00other")
	require.NotSame(t, m1, m3)

	// "\x00" separator keeps ("a","b/c") and ("a/b","c") distinct (object keys
	// may contain "/"). objectMetaRMWLock/acquireShard* build exactly these keys.
	mA, rA := k.acquire("a\x00b/c")
	mB, rB := k.acquire("a/b\x00c")
	require.NotSame(t, mA, mB, `"\x00" separator must distinguish (a, b/c) from (a/b, c)`)

	for _, rel := range []func(){r1, r2, r3, rA, rB} {
		rel()
	}
}

func TestKeyedRWMutex_BoundedAfterRelease(t *testing.T) {
	var k keyedRWMutex
	require.Equal(t, 0, k.liveLen())

	// Acquire-and-release many distinct keys: each entry must be reclaimed at zero
	// refs, so the map never accumulates (the leak this type fixes).
	for i := 0; i < 5000; i++ {
		unlock := k.lockWrite(string(rune(i)) + "key")
		unlock()
	}
	require.Equal(t, 0, k.liveLen(), "every released key must be evicted (bounded)")

	// Held keys stay resident; releasing returns to zero.
	releases := make([]func(), 8)
	for i := range releases {
		releases[i] = k.lockWrite(string(rune('a' + i)))
	}
	require.Equal(t, 8, k.liveLen())
	for _, rel := range releases {
		rel()
	}
	require.Equal(t, 0, k.liveLen())
}

// TestKeyedRWMutex_MutualExclusion is the race-detector guard: many goroutines
// contend on the same key, and the per-key lock must serialize their writes to a
// shared counter (run with -race). It also leaves the map empty at the end.
func TestKeyedRWMutex_MutualExclusion(t *testing.T) {
	var k keyedRWMutex
	const goroutines, iters = 50, 200
	counter := 0
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				unlock := k.lockWrite("hot\x00key")
				counter++ // protected by the per-key write lock
				unlock()
			}
		}()
	}
	wg.Wait()
	require.Equal(t, goroutines*iters, counter, "every increment must serialize under the per-key lock")
	require.Equal(t, 0, k.liveLen(), "lock entry must be reclaimed once all holders release")
}

// TestKeyedRWMutex_ReadWriteShare verifies read and write acquirers of the same
// key share one lock (a writer blocks readers), and the entry is reclaimed.
func TestKeyedRWMutex_ReadWriteShare(t *testing.T) {
	var k keyedRWMutex
	mr, rr := k.acquire("k")
	mw, rw := k.acquire("k")
	require.Same(t, mr, mw, "read and write acquirers of one key share the lock")
	rr()
	rw()
	require.Equal(t, 0, k.liveLen())
}
