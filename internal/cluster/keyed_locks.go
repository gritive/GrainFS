package cluster

import "sync"

// keyedRWMutex hands out a per-key *sync.RWMutex and reclaims it once the last
// holder releases, so memory is bounded to currently-held keys instead of growing
// one never-evicted entry per (bucket,key) ever seen (the prior pool.SyncMap
// behaviour — an unbounded leak on a long-running process).
//
// Safety: refs is incremented under the shard mutex BEFORE acquire returns and
// only decremented (with conditional delete at zero) under the same mutex, so an
// entry can never be evicted while any holder is between acquire and release —
// including a holder blocked on mu.Lock(). Two concurrent acquirers of the same
// key therefore always converge on the same RWMutex (mutual exclusion holds);
// a fresh entry is created only after the previous one reached zero refs and was
// deleted, by which point no one holds it. The accounting is sharded so the hot
// per-key lock path does not serialise on one global mutex.
//
// The zero value is ready to use. Callers must call the returned release exactly
// once, after unlocking the mutex.
const keyedLockAccountingShards = 64

type keyedRWMutex struct {
	shards [keyedLockAccountingShards]keyedLockShard
}

type keyedLockShard struct {
	mu sync.Mutex
	m  map[string]*refRWMutex // lazily allocated on first use
}

type refRWMutex struct {
	mu   sync.RWMutex
	refs int
}

func (k *keyedRWMutex) shardFor(key string) *keyedLockShard {
	return &k.shards[fnv32a(key)%keyedLockAccountingShards]
}

// acquire returns the RWMutex for key plus a release closure. The caller takes
// the lock it wants (Lock or RLock), does its work, unlocks, then calls release.
func (k *keyedRWMutex) acquire(key string) (*sync.RWMutex, func()) {
	sh := k.shardFor(key)
	sh.mu.Lock()
	if sh.m == nil {
		sh.m = make(map[string]*refRWMutex)
	}
	e := sh.m[key]
	if e == nil {
		e = &refRWMutex{}
		sh.m[key] = e
	}
	e.refs++
	sh.mu.Unlock()
	return &e.mu, func() {
		sh.mu.Lock()
		e.refs--
		if e.refs == 0 {
			delete(sh.m, key)
		}
		sh.mu.Unlock()
	}
}

// lockWrite acquires the per-key write lock and returns an unlock closure that
// releases the lock AND the keyed-map reference. Call it exactly once.
func (k *keyedRWMutex) lockWrite(key string) func() {
	mu, release := k.acquire(key)
	mu.Lock()
	return func() {
		mu.Unlock()
		release()
	}
}

// lockRead is lockWrite's read-lock counterpart.
func (k *keyedRWMutex) lockRead(key string) func() {
	mu, release := k.acquire(key)
	mu.RLock()
	return func() {
		mu.RUnlock()
		release()
	}
}

// fnv32a is the 32-bit FNV-1a hash, inlined to spread keys across accounting
// shards without allocating (hot path).
func fnv32a(s string) uint32 {
	const (
		offset = 2166136261
		prime  = 16777619
	)
	h := uint32(offset)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= prime
	}
	return h
}
