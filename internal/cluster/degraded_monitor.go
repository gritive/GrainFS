package cluster

import (
	"context"
	"fmt"
	"net"
	"sort"
	"time"

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
// monitor — a single fire-and-forget Send call. Defined here to avoid a
// cluster→server circular import.
type AlertSender interface {
	Send(alerts.Alert)
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
// The first check waits for the first interval tick. This avoids turning
// normal rolling startup into a sticky degraded state while peers are still
// binding their QUIC sockets.
//
// Node liveness is determined by a real QUIC shard-service ping when the
// backend has a shard service. Unit tests and partially-wired backends fall
// back to a UDP probe.
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
			m.alertSender.Send(alert)
		}
	} else {
		m.quorumLostTicks = 0
	}
}

func (m *DegradedMonitor) check() {
	nodes := m.configuredPlacementNodes()
	minRequired := m.minRequiredShards()

	// Guard 1: EC not configured at all — solo deploy.
	// Guard 2: EC configured but this deployment was never large enough to
	// actually erasure-code (e.g. single node with k=4 m=2). Use the static
	// configured node set here, not ECActive()/liveNodes(): liveNodes depends on
	// peerHealth, and after peer I/O marks nodes unhealthy this monitor must
	// still be able to enter degraded mode instead of short-circuiting healthy.
	if minRequired == 0 || !m.hasTopologyEC() && !m.backend.ecConfig.IsActive(len(nodes)) {
		m.tracker.Report(false, "", "")
		return
	}

	liveCount := m.countLiveNodes()

	// Degraded when the cluster can no longer place the configured EC stripe.
	// For the 1+0 single-node profile this threshold is 1: it has no redundancy,
	// but it still uses the EC object pipeline.
	degraded := liveCount < minRequired

	if degraded {
		m.tracker.Report(true, "ec_insufficient_nodes",
			fmt.Sprintf("live=%d min_required=%d", liveCount, minRequired))
	} else {
		m.tracker.Report(false, "", "")
	}
}

func (m *DegradedMonitor) hasTopologyEC() bool {
	return m.backend != nil && m.backend.shardGroup != nil && len(m.backend.shardGroup.ShardGroups()) > 0
}

func (m *DegradedMonitor) minRequiredShards() int {
	minRequired := m.backend.ecConfig.NumShards()
	if m.backend.shardGroup == nil {
		return minRequired
	}
	for _, group := range m.backend.shardGroup.ShardGroups() {
		if len(group.PeerIDs) == 0 {
			continue
		}
		if n := DesiredECConfigForGroup(group).NumShards(); n > minRequired {
			minRequired = n
		}
	}
	return minRequired
}

func (m *DegradedMonitor) configuredPlacementNodes() []string {
	seen := make(map[string]struct{}, len(m.backend.allNodes))
	for _, node := range m.backend.allNodes {
		if node != "" {
			seen[node] = struct{}{}
		}
	}
	if m.backend.shardGroup != nil {
		for _, group := range m.backend.shardGroup.ShardGroups() {
			for _, peer := range group.PeerIDs {
				if peer != "" {
					seen[peer] = struct{}{}
				}
			}
		}
	}
	nodes := make([]string, 0, len(seen))
	for node := range seen {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)
	return nodes
}

// countLiveNodes probes all configured nodes and returns the count of nodes
// that can accept a QUIC shard-service RPC. A UDP fallback remains for unit
// tests and partially-wired backends.
//
// As a side-effect, dead peer addresses are marked unhealthy in peerHealth so
// that clusterStatus's down_nodes computation (via liveNodes/LivePeers) stays
// accurate. PeerHealth is only used after probing, not as a short-circuit:
// startup shard I/O can mark a not-yet-ready peer unhealthy, so the existing
// cooldown must expire before an ambiguous UDP timeout is counted live again.
func (m *DegradedMonitor) countLiveNodes() int {
	nodes := m.configuredPlacementNodes()
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
			ch <- result{addr: a, alive: m.probePeer(a)}
		}(addr)
	}

	live := 0
	for range nodes {
		r := <-ch
		alive := r.alive
		if alive && r.addr != m.backend.selfAddr && m.backend.peerHealth != nil && !m.backend.peerHealth.IsHealthy(r.addr) {
			alive = false
		}
		if alive {
			live++
		} else if m.backend.peerHealth != nil {
			m.backend.peerHealth.MarkUnhealthy(r.addr)
		}
	}
	return live
}

func (m *DegradedMonitor) probePeer(addr string) bool {
	if m.backend.shardSvc != nil {
		ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
		defer cancel()
		return m.backend.shardSvc.Ping(ctx, addr) == nil
	}
	return probeUDPPort(addr, probeTimeout)
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
