package packblob

import (
	"sort"
	"sync"
)

type packedListIndex struct {
	mu      sync.RWMutex
	buckets map[string]*packedBucketList
}

type packedBucketList struct {
	keys   map[string]struct{}
	sorted []string
	dirty  bool
}

func (idx *packedListIndex) add(pk packedKey) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.ensure()
	b := idx.bucket(pk.bucket)
	if _, ok := b.keys[pk.key]; ok {
		return
	}
	b.keys[pk.key] = struct{}{}
	b.dirty = true
}

func (idx *packedListIndex) remove(pk packedKey) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if idx.buckets == nil {
		return
	}
	b := idx.buckets[pk.bucket]
	if b == nil {
		return
	}
	if _, ok := b.keys[pk.key]; !ok {
		return
	}
	delete(b.keys, pk.key)
	b.dirty = true
	if len(b.keys) == 0 {
		delete(idx.buckets, pk.bucket)
	}
}

func (idx *packedListIndex) removeBucket(bucket string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if idx.buckets != nil {
		delete(idx.buckets, bucket)
	}
}

func (idx *packedListIndex) clear() {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.buckets = nil
}

func (idx *packedListIndex) page(bucket, prefix, marker string, maxKeys int) ([]string, bool) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if idx.buckets == nil || maxKeys < 0 {
		return nil, false
	}
	b := idx.buckets[bucket]
	if b == nil || len(b.keys) == 0 {
		return nil, false
	}
	if b.dirty {
		b.sorted = b.sorted[:0]
		for key := range b.keys {
			b.sorted = append(b.sorted, key)
		}
		sort.Strings(b.sorted)
		b.dirty = false
	}
	keys := b.sorted
	startAt := prefix
	if marker > startAt {
		startAt = marker
	}
	pos := sort.SearchStrings(keys, startAt)
	if marker != "" {
		for pos < len(keys) && keys[pos] <= marker {
			pos++
		}
	}
	out := make([]string, 0, maxKeys)
	for pos < len(keys) && len(out) < maxKeys {
		key := keys[pos]
		if prefix != "" && !hasPrefix(key, prefix) {
			break
		}
		out = append(out, key)
		pos++
	}
	truncated := false
	if pos < len(keys) {
		next := keys[pos]
		truncated = prefix == "" || hasPrefix(next, prefix)
	}
	return out, truncated
}

func (idx *packedListIndex) ensure() {
	if idx.buckets == nil {
		idx.buckets = make(map[string]*packedBucketList)
	}
}

func (idx *packedListIndex) bucket(bucket string) *packedBucketList {
	b := idx.buckets[bucket]
	if b == nil {
		b = &packedBucketList{keys: make(map[string]struct{})}
		idx.buckets[bucket] = b
	}
	return b
}

func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}
