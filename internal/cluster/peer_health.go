package cluster

import (
	"sync"
	"time"
)

// PeerHealth tracks the health status of cluster peers.
// Unhealthy peers are automatically recovered after a cooldown period.
type PeerHealth struct {
	mu       sync.RWMutex
	peers    map[string]time.Time // peer -> time when marked unhealthy (zero = healthy)
	cooldown time.Duration
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
func (ph *PeerHealth) MarkUnhealthy(peer string) {
	ph.mu.Lock()
	defer ph.mu.Unlock()
	ph.peers[peer] = time.Now()
}

// MarkHealthy marks a peer as available.
func (ph *PeerHealth) MarkHealthy(peer string) {
	ph.mu.Lock()
	defer ph.mu.Unlock()
	ph.peers[peer] = time.Time{}
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
