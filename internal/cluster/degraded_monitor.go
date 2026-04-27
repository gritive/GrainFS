package cluster

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/gritive/GrainFS/internal/alerts"
)

// probeTimeout is the maximum time to wait for a UDP response when probing a
// peer's QUIC port. On a loopback interface "connection refused" arrives in
// microseconds; using 200 ms gives plenty of headroom while keeping the total
// probe round short.
const probeTimeout = 200 * time.Millisecond

// DegradedMonitor periodically checks whether live node count is sufficient
// to satisfy the EC data shard threshold and reports degraded state via the
// alerts tracker.
//
// The first check fires immediately on Start so the server knows its state
// without waiting for the first 30 s tick.
//
// Node liveness is determined by probing each peer's QUIC/Raft address via
// UDP: a "connection refused" reply means the port is closed (node dead),
// while a timeout means the UDP socket is open (node alive — QUIC won't
// respond to garbage but accepts the packet). This avoids relying on
// PeerHealth which is only updated during actual shard I/O.
type DegradedMonitor struct {
	backend  *DistributedBackend
	tracker  *alerts.DegradedTracker
	interval time.Duration
}

// NewDegradedMonitor creates a monitor that checks EC liveness every interval.
// Pass alerts.DegradedTracker directly to avoid a circular import with the
// server package.
func NewDegradedMonitor(backend *DistributedBackend, tracker *alerts.DegradedTracker, interval time.Duration) *DegradedMonitor {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	return &DegradedMonitor{
		backend:  backend,
		tracker:  tracker,
		interval: interval,
	}
}

// Run starts the monitor loop. It blocks until ctx is cancelled.
// Call in a goroutine.
func (m *DegradedMonitor) Run(ctx context.Context) {
	// Fire immediately, then every interval.
	m.check()

	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.check()
		}
	}
}

func (m *DegradedMonitor) check() {
	// If EC is not configured at all (DataShards == 0 = solo mode),
	// there is nothing to protect — never degraded.
	if m.backend.ecConfig.DataShards == 0 {
		m.tracker.Report(false, "", "")
		return
	}

	liveCount := m.countLiveNodes()

	// Degraded when the cluster can no longer form an EC stripe:
	//   • fewer than MinECNodes live → EffectiveConfig returns zero-value
	//   • fewer than the effective DataShards needed for the current size
	degraded := liveCount < MinECNodes
	if !degraded {
		threshold := EffectiveConfig(liveCount, m.backend.ecConfig).DataShards
		degraded = liveCount < threshold
	}

	if degraded {
		m.tracker.Report(true, "ec_insufficient_nodes",
			fmt.Sprintf("live=%d min_required=%d", liveCount, MinECNodes))
	} else {
		m.tracker.Report(false, "", "")
	}
}

// countLiveNodes probes all configured nodes via UDP and returns the count of
// nodes whose QUIC port is open (alive). A node is considered dead if its port
// returns ICMP "connection refused" within probeTimeout; a timeout means the
// UDP socket is accepting traffic (QUIC does not respond to garbage bytes).
//
// As a side-effect, dead peer addresses are marked unhealthy in peerHealth so
// that clusterStatus's down_nodes computation (via liveNodes/LivePeers) stays
// accurate. We only mark dead peers, never call MarkHealthy — the existing
// 10 s cooldown in PeerHealth handles recovery so we don't override signals
// from shard I/O.
func (m *DegradedMonitor) countLiveNodes() int {
	nodes := m.backend.allNodes
	if len(nodes) == 0 {
		return 0
	}

	type result struct {
		addr  string
		alive bool
	}
	ch := make(chan result, len(nodes))

	for _, addr := range nodes {
		go func(a string) {
			// Always count self as live without probing.
			if a == m.backend.selfAddr {
				ch <- result{addr: a, alive: true}
				return
			}
			ch <- result{addr: a, alive: probeUDPPort(a, probeTimeout)}
		}(addr)
	}

	live := 0
	for range nodes {
		r := <-ch
		if r.alive {
			live++
		} else if m.backend.peerHealth != nil {
			m.backend.peerHealth.MarkUnhealthy(r.addr)
		}
	}
	return live
}

// probeUDPPort checks whether a UDP port is open by sending a single byte and
// waiting for either a response (alive), "connection refused" (dead), or a
// timeout (alive — no response expected from QUIC to arbitrary bytes).
func probeUDPPort(addr string, timeout time.Duration) bool {
	conn, err := net.DialTimeout("udp", addr, timeout)
	if err != nil {
		return false
	}
	defer conn.Close()

	_ = conn.SetDeadline(time.Now().Add(timeout))
	if _, err = conn.Write([]byte{0}); err != nil {
		return false
	}

	buf := make([]byte, 1)
	_, err = conn.Read(buf)
	if err == nil {
		return true // got a response
	}
	// "connection refused" = ICMP port unreachable = port is closed = dead
	// Any other error (deadline exceeded) = port is open = alive
	netErr, ok := err.(net.Error)
	if ok && netErr.Timeout() {
		return true // timed out waiting = UDP port is open = alive
	}
	return false // connection refused or other error = dead
}
