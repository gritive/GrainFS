// internal/audit/ring.go
package audit

import "sync/atomic"

// ringCap covers ~60s × 1000 req/s bursts. Must be a power of two.
const ringCap = 1 << 16 // 65536

// Ring is a goroutine-safe bounded event buffer.
// Multiple producers call Put concurrently; a single consumer calls DrainInto.
// No heap allocations after NewRing.
type Ring struct {
	ch    chan S3Event
	drops atomic.Uint64
}

func NewRing() *Ring { return &Ring{ch: make(chan S3Event, ringCap)} }

// Put enqueues an event. Drops and increments the counter if the ring is full. Never blocks.
func (r *Ring) Put(e S3Event) {
	select {
	case r.ch <- e:
	default:
		r.drops.Add(1)
	}
}

// DrainInto drains all buffered events into dst.
// When dst is nil or has zero capacity, events are appended without bound (allocates).
// When dst has a positive capacity, at most cap(dst) events are drained (no allocation).
func (r *Ring) DrainInto(dst []S3Event) []S3Event {
	if cap(dst) == 0 {
		for {
			select {
			case e := <-r.ch:
				dst = append(dst, e)
			default:
				return dst
			}
		}
	}
	dst = dst[:0]
	for len(dst) < cap(dst) {
		select {
		case e := <-r.ch:
			dst = append(dst, e)
		default:
			return dst
		}
	}
	return dst
}

// Drops returns the cumulative drop count since creation.
func (r *Ring) Drops() uint64 { return r.drops.Load() }

// Len returns the current number of buffered events (approximate, for monitoring).
func (r *Ring) Len() int { return len(r.ch) }
