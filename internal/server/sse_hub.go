package server

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gritive/GrainFS/internal/pool"
)

// Event is a typed message broadcast to SSE clients.
type Event struct {
	Type string // "metric", "log", "cluster", "heal"
	Data []byte // JSON payload
}

// subscriber holds the receive channel and an optional category filter.
// An empty categories map means "all events" (backward-compatible default).
type subscriber struct {
	ch         chan Event
	categories map[string]struct{}
}

// matches reports whether s should receive an event of the given type.
func (s *subscriber) matches(eventType string) bool {
	if len(s.categories) == 0 {
		return true
	}
	_, ok := s.categories[eventType]
	return ok
}

// Hub fan-outs Events to all active SSE subscribers.
type Hub struct {
	clients pool.SyncMap[string, *subscriber]
	idSeq   atomic.Uint64
	count   atomic.Int64 // number of active subscribers

	dropMu sync.Mutex
	drops  map[string]uint64 // per-category dropped event counts
}

// HasSubscribers reports whether any SSE clients are currently connected.
func (h *Hub) HasSubscribers() bool { return h.count.Load() > 0 }

// NewHub returns a ready Hub.
func NewHub() *Hub {
	return &Hub{drops: map[string]uint64{}}
}

// Subscribe registers a new SSE client. With no categories, the subscriber
// receives every event (legacy behavior). When one or more categories are
// provided, only events with a matching Event.Type are delivered.
func (h *Hub) Subscribe(categories ...string) (id string, ch <-chan Event, cancel func()) {
	seq := h.idSeq.Add(1)
	id = fmt.Sprintf("client-%d", seq)
	c := make(chan Event, 8)
	sub := &subscriber{ch: c}
	if len(categories) > 0 {
		sub.categories = make(map[string]struct{}, len(categories))
		for _, cat := range categories {
			sub.categories[cat] = struct{}{}
		}
	}
	h.clients.Store(id, sub)
	h.count.Add(1)
	cancel = func() {
		if v, ok := h.clients.LoadAndDelete(id); ok {
			h.count.Add(-1)
			close(v.ch)
		}
	}
	return id, c, cancel
}

// Broadcast sends e to every active subscriber whose category filter matches
// and returns the number of matching subscribers whose buffer was full (i.e.
// events dropped for this single call). Drops are also tallied into the Hub's
// per-category counter, readable via DroppedCount. Callers that need exact
// drop attribution (e.g. Prometheus) should use the return value directly —
// the counter diff is racy under concurrent emit.
func (h *Hub) Broadcast(e Event) (dropped int) {
	h.clients.Range(func(_ string, sub *subscriber) bool {
		if !sub.matches(e.Type) {
			return true
		}
		select {
		case sub.ch <- e:
		default:
			dropped++
		}
		return true
	})
	if dropped > 0 {
		h.dropMu.Lock()
		h.drops[e.Type] += uint64(dropped)
		h.dropMu.Unlock()
	}
	return dropped
}

// DroppedCount returns the total number of events dropped for the given
// category on this Hub since it was created.
func (h *Hub) DroppedCount(category string) uint64 {
	h.dropMu.Lock()
	defer h.dropMu.Unlock()
	return h.drops[category]
}

// WriteSSE streams events from hub to w until ctx is cancelled.
// Callers are responsible for setting SSE headers before calling this.
// With no categories, every event is streamed (legacy behavior).
//
// On entry the function writes a single SSE comment line so that frameworks
// which buffer the response body (e.g. Hertz's SetBodyStream) flush their
// headers immediately. Without this an EventSource client receives nothing —
// not even the headers — until the first event arrives, which can be never
// for a quiet category like "heal".
func (h *Hub) WriteSSE(ctx context.Context, w io.Writer, categories ...string) {
	_, ch, cancel := h.Subscribe(categories...)
	defer cancel()

	// SSE comment lines start with ":" and are ignored by clients (per spec).
	// They serve as a header-flush nudge and as a keep-alive heartbeat.
	fmt.Fprint(w, ": ok\n\n") //nolint:errcheck

	keepalive := time.NewTicker(15 * time.Second)
	defer keepalive.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-keepalive.C:
			fmt.Fprint(w, ": keep-alive\n\n") //nolint:errcheck
		case e, ok := <-ch:
			if !ok {
				return
			}
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", e.Type, e.Data) //nolint:errcheck
		}
	}
}
