package server

import (
	"encoding/json"
	"log/slog"

	"github.com/gritive/GrainFS/internal/eventstore"
	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/scrubber"
)

// healEvCategory is the SSE category string used for heal events. Subscribers
// to /api/events/heal/stream filter by this category.
const healEvCategory = "heal"

// healEmitter implements scrubber.Emitter by:
//
//  1. Updating Prometheus counters/histograms (always)
//  2. Broadcasting the event over the SSE Hub (if hub != nil) under category "heal"
//  3. Persisting the event into the eventstore (if evStore != nil) so the
//     dashboard can render history after a reconnect.
//
// All three sinks are best-effort: a slow SSE client drops the event, an
// eventstore write failure is logged but never blocks the scrubber. This
// preserves the iron rule from Phase 13: scrubber latency must not be coupled
// to BadgerDB write latency.
type healEmitter struct {
	hub        *Hub
	enqueueEvt func(eventstore.Event) // s.emitEvent — bounded queue, non-blocking
}

// newHealEmitter returns an emitter wired to the given hub and event-store enqueue
// function. Either may be nil; the resulting emitter degrades gracefully.
func newHealEmitter(hub *Hub, enqueue func(eventstore.Event)) *healEmitter {
	return &healEmitter{hub: hub, enqueueEvt: enqueue}
}

// Emit publishes a HealEvent to all active sinks.
func (e *healEmitter) Emit(ev scrubber.HealEvent) {
	metrics.HealEventsTotal.WithLabelValues(string(ev.Phase), string(ev.Outcome)).Inc()
	if ev.DurationMs > 0 {
		metrics.HealDurationMs.WithLabelValues(string(ev.Phase)).Observe(float64(ev.DurationMs))
	}

	data, err := json.Marshal(ev)
	if err != nil {
		// Marshal failure is unexpected (HealEvent is a flat struct) but we
		// still want to know about it. Drop the event rather than block.
		slog.Warn("heal: marshal failed", "err", err, "phase", ev.Phase)
		return
	}

	if e.hub != nil {
		before := e.hub.DroppedCount(healEvCategory)
		e.hub.Broadcast(Event{Type: healEvCategory, Data: data})
		if after := e.hub.DroppedCount(healEvCategory); after > before {
			metrics.HealStreamDroppedTotal.Add(float64(after - before))
		}
	}

	if e.enqueueEvt != nil {
		e.enqueueEvt(eventstore.Event{
			Type:   healEvCategory,
			Action: string(ev.Phase),
			Bucket: ev.Bucket,
			Key:    ev.Key,
			Metadata: map[string]any{
				"id":             ev.ID,
				"phase":          string(ev.Phase),
				"outcome":        string(ev.Outcome),
				"shard_id":       ev.ShardID,
				"peer_id":        ev.PeerID,
				"bytes_repaired": ev.BytesRepaired,
				"duration_ms":    ev.DurationMs,
				"err_code":       ev.ErrCode,
				"correlation_id": ev.CorrelationID,
				"version_id":     ev.VersionID,
			},
		})
	}
}
