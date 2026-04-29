package pool

import "sync"

// SyncMap is a type-safe generic wrapper around sync.Map.
type SyncMap[K comparable, V any] struct {
	m sync.Map
}

func (s *SyncMap[K, V]) Load(key K) (V, bool) {
	v, ok := s.m.Load(key)
	if !ok {
		var zero V
		return zero, false
	}
	return v.(V), true
}

func (s *SyncMap[K, V]) Store(key K, val V) { s.m.Store(key, val) }

func (s *SyncMap[K, V]) Delete(key K) { s.m.Delete(key) }

func (s *SyncMap[K, V]) LoadOrStore(key K, val V) (V, bool) {
	actual, loaded := s.m.LoadOrStore(key, val)
	return actual.(V), loaded
}

func (s *SyncMap[K, V]) LoadAndDelete(key K) (V, bool) {
	v, ok := s.m.LoadAndDelete(key)
	if !ok {
		var zero V
		return zero, false
	}
	return v.(V), ok
}

func (s *SyncMap[K, V]) Range(fn func(K, V) bool) {
	s.m.Range(func(k, v any) bool {
		return fn(k.(K), v.(V))
	})
}
