package serveruntime

import (
	"context"
	"sync"

	"github.com/gritive/GrainFS/internal/cluster"
)

type placementMonitorEntry struct {
	backend *cluster.GroupBackend
	cancel  context.CancelFunc
	seen    bool
}

// PlacementMonitorRegistry tracks per-group ShardPlacementMonitor goroutines
// across membership refresh ticks. refresh() starts new monitors for groups
// that just appeared, replaces ones whose backend pointer changed (group
// re-instantiated), and cancels monitors for groups that left.
type PlacementMonitorRegistry struct {
	mu      sync.Mutex
	entries map[string]placementMonitorEntry
}

// NewPlacementMonitorRegistry returns an empty registry.
func NewPlacementMonitorRegistry() *PlacementMonitorRegistry {
	return &PlacementMonitorRegistry{entries: make(map[string]placementMonitorEntry)}
}

// Refresh reconciles the registry against the current set of groups. Each
// group whose backend pointer differs from a stored entry is treated as a
// fresh instantiation: the prior monitor is cancelled and start() is called
// with a new ctx.
func (r *PlacementMonitorRegistry) Refresh(parent context.Context, groups []*cluster.DataGroup, start func(context.Context, *cluster.DataGroup)) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for id, entry := range r.entries {
		entry.seen = false
		r.entries[id] = entry
	}
	for _, dg := range groups {
		if dg == nil || dg.Backend() == nil {
			continue
		}
		id := dg.ID()
		gb := dg.Backend()
		if entry, ok := r.entries[id]; ok {
			if entry.backend == gb {
				entry.seen = true
				r.entries[id] = entry
				continue
			}
			entry.cancel()
		}
		ctx, cancel := context.WithCancel(parent)
		r.entries[id] = placementMonitorEntry{backend: gb, cancel: cancel, seen: true}
		start(ctx, dg)
	}
	for id, entry := range r.entries {
		if entry.seen {
			continue
		}
		entry.cancel()
		delete(r.entries, id)
	}
}
