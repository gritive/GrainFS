package main

import (
	"context"
	"sync"

	"github.com/gritive/GrainFS/internal/cluster"
)

type reshardManagerEntry struct {
	backend *cluster.GroupBackend
	cancel  context.CancelFunc
	seen    bool
}

type reshardManagerRegistry struct {
	mu      sync.Mutex
	entries map[string]reshardManagerEntry
}

func newReshardManagerRegistry() *reshardManagerRegistry {
	return &reshardManagerRegistry{entries: make(map[string]reshardManagerEntry)}
}

func (r *reshardManagerRegistry) refresh(parent context.Context, groups []*cluster.DataGroup, start func(context.Context, *cluster.DataGroup)) {
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
		r.entries[id] = reshardManagerEntry{backend: gb, cancel: cancel, seen: true}
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
