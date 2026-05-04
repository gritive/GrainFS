package cluster

import (
	"sort"
	"sync"
	"time"

	"github.com/gritive/GrainFS/internal/metrics"
)

// PeerHealth tracks the health status of cluster peers.
// Unhealthy peers are automatically recovered after a cooldown period.
type PeerHealth struct {
	mu       sync.RWMutex
	peers    map[string]time.Time // peer -> time when marked unhealthy (zero = healthy)
	cooldown time.Duration
}

// PeerHealthEntry is the externally-visible snapshot of a peer's health state.
// Returned by Snapshot for admin/observability surfaces.
type PeerHealthEntry struct {
	ID                  string     `json:"id"`
	Healthy             bool       `json:"healthy"`
	LastFailure         *time.Time `json:"last_failure,omitempty"`
	CooldownRemainingMs int64      `json:"cooldown_remaining_ms"`
}

// NewPeerHealth creates a health tracker with the given cooldown duration.
// After being marked unhealthy, a peer becomes eligible for retry after cooldown.
func NewPeerHealth(peers []string, cooldown time.Duration) *PeerHealth {
	ph := &PeerHealth{
		peers:    make(map[string]time.Time, len(peers)),
		cooldown: cooldown,
	}
	for _, p := range peers {
		ph.peers[p] = time.Time{} // healthy
	}
	return ph
}

// IsHealthy returns true if the peer is healthy or the cooldown has expired.
func (ph *PeerHealth) IsHealthy(peer string) bool {
	ph.mu.RLock()
	defer ph.mu.RUnlock()

	t, ok := ph.peers[peer]
	if !ok {
		return true // unknown peers are assumed healthy
	}
	if t.IsZero() {
		return true
	}
	return time.Since(t) >= ph.cooldown
}

// MarkUnhealthy marks a peer as temporarily unavailable.
// Returns true if this call transitioned the peer from healthy to unhealthy.
// The transition signal lets callers emit a one-shot warn log per transition
// instead of logging every skip. Side effect: sets the
// `grainfs_peer_unhealthy{peer}` Gauge to 1 on transition.
func (ph *PeerHealth) MarkUnhealthy(peer string) (transitioned bool) {
	ph.mu.Lock()
	prev, ok := ph.peers[peer]
	wasHealthy := !ok || prev.IsZero() || time.Since(prev) >= ph.cooldown
	ph.peers[peer] = time.Now()
	ph.mu.Unlock()
	if wasHealthy {
		metrics.PeerUnhealthy.WithLabelValues(peer).Set(1)
	}
	return wasHealthy
}

// MarkHealthy marks a peer as available.
// Returns true if this call transitioned the peer from unhealthy to healthy.
// Side effect: sets the `grainfs_peer_unhealthy{peer}` Gauge to 0 on transition.
func (ph *PeerHealth) MarkHealthy(peer string) (transitioned bool) {
	ph.mu.Lock()
	prev, ok := ph.peers[peer]
	wasUnhealthy := ok && !prev.IsZero() && time.Since(prev) < ph.cooldown
	ph.peers[peer] = time.Time{}
	ph.mu.Unlock()
	if wasUnhealthy {
		metrics.PeerUnhealthy.WithLabelValues(peer).Set(0)
	}
	return wasUnhealthy
}

// HealthyPeers returns the list of currently healthy peers.
func (ph *PeerHealth) HealthyPeers() []string {
	ph.mu.RLock()
	defer ph.mu.RUnlock()

	var result []string
	for p, t := range ph.peers {
		if t.IsZero() || time.Since(t) >= ph.cooldown {
			result = append(result, p)
		}
	}
	return result
}

// Snapshot returns the current health state of every tracked peer, sorted by
// peer ID for deterministic output. Used by admin endpoints to surface peer
// health to operators.
func (ph *PeerHealth) Snapshot() []PeerHealthEntry {
	ph.mu.RLock()
	defer ph.mu.RUnlock()

	out := make([]PeerHealthEntry, 0, len(ph.peers))
	now := time.Now()
	for p, t := range ph.peers {
		entry := PeerHealthEntry{ID: p}
		if t.IsZero() {
			entry.Healthy = true
		} else {
			elapsed := now.Sub(t)
			if elapsed >= ph.cooldown {
				entry.Healthy = true
			} else {
				entry.Healthy = false
				lf := t
				entry.LastFailure = &lf
				entry.CooldownRemainingMs = (ph.cooldown - elapsed).Milliseconds()
			}
		}
		out = append(out, entry)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
