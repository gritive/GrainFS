package p9server

import (
	"sort"
	"sync"
)

type objectLocks struct {
	mu    sync.Mutex
	locks map[string]*objectLockEntry
}

func newObjectLocks() *objectLocks {
	return &objectLocks{locks: make(map[string]*objectLockEntry)}
}

func (l *objectLocks) lock(bucket, key string) func() {
	if l == nil {
		return func() {}
	}
	name := bucket + "\x00" + key
	l.mu.Lock()
	entry := l.locks[name]
	if entry == nil {
		entry = &objectLockEntry{}
		l.locks[name] = entry
	}
	entry.refs++
	l.mu.Unlock()
	entry.mu.Lock()
	return func() {
		entry.mu.Unlock()
		l.mu.Lock()
		entry.refs--
		if entry.refs == 0 {
			delete(l.locks, name)
		}
		l.mu.Unlock()
	}
}

func (l *objectLocks) lockMany(keys ...objectLockKey) func() {
	if l == nil || len(keys) == 0 {
		return func() {}
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].bucket == keys[j].bucket {
			return keys[i].key < keys[j].key
		}
		return keys[i].bucket < keys[j].bucket
	})
	unlocks := make([]func(), 0, len(keys))
	var prev objectLockKey
	for i, key := range keys {
		if i > 0 && key == prev {
			continue
		}
		unlocks = append(unlocks, l.lock(key.bucket, key.key))
		prev = key
	}
	return func() {
		for i := len(unlocks) - 1; i >= 0; i-- {
			unlocks[i]()
		}
	}
}

type objectLockKey struct {
	bucket string
	key    string
}

type objectLockEntry struct {
	mu   sync.Mutex
	refs int
}
