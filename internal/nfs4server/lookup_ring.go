package nfs4server

import (
	"sync"
	"time"
)

type LookupRecord struct {
	Client string
	Bucket string
	Result string
	At     time.Time
}

type LookupRing struct {
	mu  sync.Mutex
	buf []LookupRecord
	pos int
}

func NewLookupRing(capacity int) *LookupRing {
	if capacity <= 0 {
		capacity = 1
	}
	return &LookupRing{buf: make([]LookupRecord, capacity)}
}

func (r *LookupRing) Record(rec LookupRecord) {
	if r == nil {
		return
	}
	if rec.At.IsZero() {
		rec.At = time.Now()
	}
	r.mu.Lock()
	r.buf[r.pos] = rec
	r.pos = (r.pos + 1) % len(r.buf)
	r.mu.Unlock()
}

func (r *LookupRing) Snapshot(bucket string, window time.Duration) []LookupRecord {
	if r == nil {
		return nil
	}
	cutoff := time.Now().Add(-window)
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]LookupRecord, 0)
	for i := 0; i < len(r.buf); i++ {
		idx := (r.pos - 1 - i + len(r.buf)) % len(r.buf)
		rec := r.buf[idx]
		if rec.At.IsZero() {
			continue
		}
		if window > 0 && rec.At.Before(cutoff) {
			continue
		}
		if rec.Bucket == bucket {
			out = append(out, rec)
		}
	}
	return out
}
