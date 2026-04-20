package server

import (
	"context"
	"encoding/json"
	"log/slog"
)

// BroadcastHandler wraps another slog.Handler and fans out each log record
// to the SSE Hub so dashboard clients receive a live log stream.
type BroadcastHandler struct {
	next slog.Handler
	hub  *Hub
}

// NewBroadcastHandler chains h with hub fan-out.
func NewBroadcastHandler(next slog.Handler, hub *Hub) *BroadcastHandler {
	return &BroadcastHandler{next: next, hub: hub}
}

func (b *BroadcastHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return b.next.Enabled(ctx, level)
}

func (b *BroadcastHandler) Handle(ctx context.Context, r slog.Record) error {
	if err := b.next.Handle(ctx, r); err != nil {
		return err
	}

	m := map[string]any{
		"level": r.Level.String(),
		"msg":   r.Message,
		"time":  r.Time.Unix(),
	}
	r.Attrs(func(a slog.Attr) bool {
		m[a.Key] = a.Value.Any()
		return true
	})
	data, err := json.Marshal(m)
	if err != nil {
		return nil
	}
	b.hub.Broadcast(Event{Type: "log", Data: data})
	return nil
}

func (b *BroadcastHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &BroadcastHandler{next: b.next.WithAttrs(attrs), hub: b.hub}
}

func (b *BroadcastHandler) WithGroup(name string) slog.Handler {
	return &BroadcastHandler{next: b.next.WithGroup(name), hub: b.hub}
}
