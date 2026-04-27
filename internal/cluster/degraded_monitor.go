package cluster

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/alerts"
	"github.com/gritive/GrainFS/internal/raft"
)

// RaftStateProvider is the minimal slice of *raft.Node used by the quorum
// monitor: just current role and leader id. Defined here as an interface so
// tests can drive the monitor without spinning up a full Raft instance.
type RaftStateProvider interface {
	State() raft.NodeState
	LeaderID() string
}

// AlertSender is the minimal slice of *server.AlertsState used by the quorum
// monitor — a single Send call. Defined here to avoid a cluster→server
// circular import.
type AlertSender interface {
	Send(alerts.Alert) error
}

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

	// Quorum monitoring (optional — nil when running without raft, e.g. solo).
	raftNode    RaftStateProvider
	alertSender AlertSender
	// quorumLostTicks counts consecutive ticks where State==Follower &&
	// LeaderID=="". Alert fires once when it crosses quorumAlertThreshold,
	// then resets to avoid spamming. Cleared the moment a leader appears.
	quorumLostTicks      int
	quorumAlertThreshold int
}

// NewDegradedMonitor creates a monitor that checks EC liveness every interval.
// Pass alerts.DegradedTracker directly to avoid a circular import with the
// server package.
func NewDegradedMonitor(backend *DistributedBackend, tracker *alerts.DegradedTracker, interval time.Duration) *DegradedMonitor {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	return &DegradedMonitor{
		backend:              backend,
		tracker:              tracker,
		interval:             interval,
		quorumAlertThreshold: 2, // ~60 s with default 30 s tick — absorbs startup leader-election jitter
	}
}

// WithQuorumCheck wires the optional Raft quorum-lost monitor. When the node
// is a follower with no leader for quorumAlertThreshold consecutive ticks
// (default 2 → ~60 s with 30 s interval), a critical alert is dispatched.
// QuorumMinMatchIndex() is intentionally NOT used: it is a GC watermark and
// can lag indefinitely on a write-quiet cluster, producing false positives.
func (m *DegradedMonitor) WithQuorumCheck(node RaftStateProvider, sender AlertSender) *DegradedMonitor {
	m.raftNode = node
	m.alertSender = sender
	return m
}

// Run starts the monitor loop. It blocks until ctx is cancelled.
// Call in a goroutine.
func (m *DegradedMonitor) Run(ctx context.Context) {
	// Fire immediately, then every interval.
	m.check()
	m.checkQuorum()

	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.check()
			m.checkQuorum()
		}
	}
}

// checkQuorum fires a critical alert when this node has been a follower
// with no known leader for quorumAlertThreshold consecutive ticks.
// No-op when WithQuorumCheck has not been wired.
func (m *DegradedMonitor) checkQuorum() {
	if m.raftNode == nil || m.alertSender == nil {
		return
	}
	if m.raftNode.State() == raft.Follower && m.raftNode.LeaderID() == "" {
		m.quorumLostTicks++
		if m.quorumLostTicks == m.quorumAlertThreshold {
			alert := alerts.Alert{
				Type:     "raft_quorum_lost",
				Severity: alerts.SeverityCritical,
				Resource: "cluster",
				Message:  fmt.Sprintf("no leader for %d consecutive %s ticks — quorum likely lost", m.quorumAlertThreshold, m.interval),
			}
			if err := m.alertSender.Send(alert); err != nil {
				log.Warn().Err(err).Msg("quorum-lost alert send failed")
			}
		}
	} else {
		m.quorumLostTicks = 0
	}
}

func (m *DegradedMonitor) check() {
	// Guard 1: EC not configured at all (DataShards == 0) — solo deploy.
	// Guard 2: EC configured but cluster too small to actually erasure-code
	// (e.g. single node with k=4 m=2). ECActive() handles this — without the
	// guard we'd false-positive every single-node test that has EC configured.
	if m.backend.ecConfig.DataShards == 0 || !m.backend.ECActive() {
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
