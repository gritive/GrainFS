package pool

import "sync"

// Pool is a type-safe generic wrapper around sync.Pool.
type Pool[T any] struct {
	p sync.Pool
}

// New returns a Pool that uses fn to create new values.
func New[T any](fn func() T) *Pool[T] {
	return &Pool[T]{p: sync.Pool{New: func() any { return fn() }}}
}

// Get returns a value from the pool.
func (p *Pool[T]) Get() T { return p.p.Get().(T) }

// Put returns v to the pool.
func (p *Pool[T]) Put(v T) { p.p.Put(v) }
