package server

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
)

// Event is a typed message broadcast to SSE clients.
type Event struct {
	Type string // "metric", "log", "cluster"
	Data []byte // JSON payload
}

// Hub fan-outs Events to all active SSE subscribers.
type Hub struct {
	clients sync.Map // id string → chan Event
	idSeq   atomic.Uint64
}

// NewHub returns a ready Hub.
func NewHub() *Hub {
	return &Hub{}
}

// Subscribe registers a new SSE client. Returns a unique id, a receive-only
// channel, and a cancel function that must be called when the client disconnects.
func (h *Hub) Subscribe() (id string, ch <-chan Event, cancel func()) {
	seq := h.idSeq.Add(1)
	id = fmt.Sprintf("client-%d", seq)
	c := make(chan Event, 8)
	h.clients.Store(id, c)
	cancel = func() {
		h.clients.Delete(id)
		close(c)
	}
	return id, c, cancel
}

// Broadcast sends e to every active subscriber. Slow clients are dropped
// (non-blocking send) to avoid head-of-line blocking.
func (h *Hub) Broadcast(e Event) {
	h.clients.Range(func(_, v any) bool {
		ch := v.(chan Event)
		select {
		case ch <- e:
		default:
		}
		return true
	})
}

// WriteSSE streams events from hub to w until ctx is cancelled.
// Callers are responsible for setting SSE headers before calling this.
func (h *Hub) WriteSSE(ctx context.Context, w io.Writer) {
	_, ch, cancel := h.Subscribe()
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return
		case e, ok := <-ch:
			if !ok {
				return
			}
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", e.Type, e.Data) //nolint:errcheck
		}
	}
}
