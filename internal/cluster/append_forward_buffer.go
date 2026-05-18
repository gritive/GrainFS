package cluster

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

// ErrForwardBufferFull signals the AppendObject forward buffer pool cannot
// accommodate this request right now. Caller maps to 503 SlowDown +
// Retry-After.
var ErrForwardBufferFull = errors.New("cluster: append forward buffer full")

// appendForwardBuffer is a byte-based semaphore: each Acquire reserves the
// requested byte count from a fixed pool; Release returns it. Reservation
// fails immediately (no blocking) when the requested size exceeds the
// remaining capacity OR exceeds the total pool capacity. The pool is
// non-blocking by design — overload should surface as 503 to the caller,
// not as a stalled forward.
type appendForwardBuffer struct {
	capacity int64
	mu       sync.Mutex
	used     int64
	inflight atomic.Int64 // mirror of `used` for lock-free gauge reads
}

func newAppendForwardBuffer(capacity int64) *appendForwardBuffer {
	return &appendForwardBuffer{capacity: capacity}
}

// Acquire reserves n bytes from the pool. Returns ErrForwardBufferFull when
// the reservation cannot fit. ctx is accepted for symmetry with future
// blocking variants but currently is not waited on (non-blocking design).
func (s *appendForwardBuffer) Acquire(ctx context.Context, n int64) error {
	if n <= 0 {
		return nil
	}
	if n > s.capacity {
		return ErrForwardBufferFull
	}
	s.mu.Lock()
	if s.used+n > s.capacity {
		s.mu.Unlock()
		return ErrForwardBufferFull
	}
	s.used += n
	s.inflight.Store(s.used)
	s.mu.Unlock()
	return nil
}

func (s *appendForwardBuffer) Release(n int64) {
	if n <= 0 {
		return
	}
	s.mu.Lock()
	s.used -= n
	if s.used < 0 {
		s.used = 0
	}
	s.inflight.Store(s.used)
	s.mu.Unlock()
}

func (s *appendForwardBuffer) InflightBytes() int64 {
	return s.inflight.Load()
}
