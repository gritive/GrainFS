package pool

import "sync/atomic"

// AtomicValue is a type-safe generic wrapper around atomic.Value.
// Load returns the zero value of T if nothing has been stored yet.
type AtomicValue[T any] struct {
	v atomic.Value
}

func (a *AtomicValue[T]) Load() T {
	v := a.v.Load()
	if v == nil {
		var zero T
		return zero
	}
	return v.(T)
}

func (a *AtomicValue[T]) Store(val T) { a.v.Store(val) }
