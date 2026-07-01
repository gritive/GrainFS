package cluster

import (
	"context"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/gossip"
	"github.com/gritive/GrainFS/internal/metrics"
)

// balancerChanBuf is the channel buffer for actor message delivery.
// Sized to absorb burst status requests without blocking callers.
const balancerChanBuf = 64

// BalancerConfig holds tunable parameters for the BalancerProposer.
// All fields can be injected at construction time; tests use small values to speed up loops.
type BalancerConfig struct {
	GossipInterval      time.Duration
	WarmupTimeout       time.Duration
	ImbalanceTriggerPct float64 // start migration when max-min disk diff exceeds this
	ImbalanceStopPct    float64 // clear active hysteresis when max-min disk diff drops below this
	MigrationRate       int     // legacy inert knob from the retired shard migration path
	LeaderTenureMin     time.Duration
	LeaderLoadThreshold float64 // leader's requestsPerSec / median before transfer
	// GracePeriod is how long after a node joins before it's counted toward the normal
	// imbalance trigger. During this window the trigger is relaxed by 1.5× to avoid
	// false active states caused by newly-added nodes.
	GracePeriod time.Duration
	// PeerSeenWindow is the maximum age of a peer's last gossip for warmupComplete.
	// Peers with UpdatedAt older than this are treated as not-yet-gossiped.
	// Defaults to 2× GossipInterval when zero.
	PeerSeenWindow time.Duration
	// CBThreshold is the disk-used percentage at which a destination node's circuit
	// breaker opens (0–1 fraction, e.g. 0.90 = 90%). Default 0.90.
	CBThreshold float64
	// MigrationMaxRetries is the maximum number of shard write attempts per shard.
	// Default 3.
	MigrationMaxRetries int
	// MigrationPendingTTL is how long a pending migration may linger before being
	// cancelled. Default 5 minutes.
	MigrationPendingTTL time.Duration
	// MigrationProposalRate is the token-bucket rate (proposals/second) for the
	// migration priority queue. Default 2.0.
	MigrationProposalRate float64
	// StickyDonorHoldTime is how long to keep using the same src node before
	// considering a switch. Default 30s.
	StickyDonorHoldTime time.Duration
}

// DefaultBalancerConfig returns production-safe defaults.
func DefaultBalancerConfig() BalancerConfig {
	return BalancerConfig{
		GossipInterval:        30 * time.Second,
		WarmupTimeout:         60 * time.Second,
		ImbalanceTriggerPct:   20.0,
		ImbalanceStopPct:      5.0,
		MigrationRate:         1,
		LeaderTenureMin:       5 * time.Minute,
		LeaderLoadThreshold:   1.3,
		GracePeriod:           10 * time.Minute,
		PeerSeenWindow:        60 * time.Second, // 2× default GossipInterval
		CBThreshold:           0.90,
		MigrationMaxRetries:   3,
		MigrationPendingTTL:   5 * time.Minute,
		MigrationProposalRate: 2.0,
		StickyDonorHoldTime:   30 * time.Second,
	}
}

// RaftBalancerNode is the subset of raft.Node used by the balancer.
type RaftBalancerNode interface {
	Propose(data []byte) error
	IsLeader() bool
	NodeID() string
	PeerIDs() []string
	TransferLeadership() error
}

// BalancerClusterCfg is the minimal cluster-config surface the balancer reads
// each tick. Production: *ClusterConfig (returns its getters); tests inject a
// stub. Hot-reload: balancer reads these every tick, so PATCH /admin/cluster-config
// changes take effect on the next gossip interval.
type BalancerClusterCfg interface {
	BalancerEnabled() bool
	BalancerImbalanceTriggerPct() float64
	BalancerImbalanceStopPct() float64
	BalancerMigrationRate() int32
	BalancerLeaderTenureMin() time.Duration
	BalancerWarmupTimeout() time.Duration
	BalancerCBThreshold() float64
	BalancerMigrationMaxRetries() int32
	BalancerMigrationPendingTTL() time.Duration
	BalancerGossipInterval() time.Duration
}

type balancerMsgKind int

const (
	msgBalancerStatus balancerMsgKind = iota
)

type balancerMsg struct {
	kind    balancerMsgKind
	replyCh chan BalancerStatus
}

// BalancerProposer monitors gossip.NodeStatsStore for disk skew and load signals.
// The old Raft-backed shard migration proposal path is retired; the actor now
// keeps status/metrics current without proposing data movement commands.
type BalancerProposer struct {
	nodeID     string
	store      *gossip.NodeStatsStore
	node       RaftBalancerNode
	clusterCfg BalancerClusterCfg
	// Non-cluster (still hardcoded) defaults — read once at construction.
	leaderLoadThreshold float64
	gracePeriod         time.Duration
	peerSeenWindow      time.Duration

	// leaderLoadTransferEnabled gates the load-driven meta-Raft leadership
	// transfer (selectPeerByLoad → TransferLeadership). Default false: this
	// behavior has never run in production (RequestsPerSec was always 0 until the
	// request-rate collector landed), and transferring control-plane leadership in
	// response to a data-plane S3 load signal is unvalidated and risks election
	// churn. Enabled in code (SetLeaderLoadTransferEnabled) once validated.
	leaderLoadTransferEnabled bool

	active    atomic.Bool // hysteresis state: true once trigger fired, false after stop threshold
	startedAt time.Time
	cbs       map[string]*circuitBreaker // per-peer CBs; keyed by peer nodeID
	logger    zerolog.Logger
	ch        chan balancerMsg // inbound messages to the actor loop
	stopCh    chan struct{}    // closed by Stop()
	stopOnce  sync.Once

	// tickCount is incremented at the end of each tickOnce call (test-only
	// observability for hot-reload tests).
	tickCount atomic.Int64
}

// NewBalancerProposer creates a BalancerProposer that reads tunables from
// clusterCfg at every tick (hot-reload). The non-cluster fields
// (LeaderLoadThreshold / GracePeriod / PeerSeenWindow) come from
// DefaultBalancerConfig() and are fixed for the lifetime of the proposer.
func NewBalancerProposer(nodeID string, store *gossip.NodeStatsStore, node RaftBalancerNode, clusterCfg BalancerClusterCfg) *BalancerProposer {
	def := DefaultBalancerConfig()
	return &BalancerProposer{
		nodeID:              nodeID,
		store:               store,
		node:                node,
		clusterCfg:          clusterCfg,
		leaderLoadThreshold: def.LeaderLoadThreshold,
		gracePeriod:         def.GracePeriod,
		peerSeenWindow:      def.PeerSeenWindow,
		startedAt:           time.Now(),
		cbs:                 make(map[string]*circuitBreaker),
		logger:              log.With().Str("component", "balancer").Logger(),
		ch:                  make(chan balancerMsg, balancerChanBuf),
		stopCh:              make(chan struct{}),
	}
}

// syncCB updates (or creates) per-peer circuit breakers from the latest gossip stats.
// Must be called from the actor goroutine only.
func (p *BalancerProposer) syncCB(peers []gossip.NodeStats) {
	thresholdPct := p.clusterCfg.BalancerCBThreshold() * 100
	for _, ns := range peers {
		if ns.NodeID == p.nodeID {
			continue // skip self
		}
		cb, ok := p.cbs[ns.NodeID]
		if !ok {
			cb = newCircuitBreaker()
			p.cbs[ns.NodeID] = cb
		}
		cb.update(ns, thresholdPct)
		if !cb.allow() {
			metrics.BalancerCBOpen.WithLabelValues(ns.NodeID).Set(1)
		} else {
			metrics.BalancerCBOpen.WithLabelValues(ns.NodeID).Set(0)
		}
	}
}

// getCB returns the circuitBreaker for nodeID, or nil if not found.
// Used for testing only — must be called from the actor goroutine (or single-threaded tests).
//
//nolint:unused // package tests inspect breaker state through this helper.
func (p *BalancerProposer) getCB(nodeID string) *circuitBreaker {
	return p.cbs[nodeID]
}

// selectDstNode returns the lightest peer that has an open (allow=true) circuit breaker.
// Returns ("", false) if no eligible peer exists.
func (p *BalancerProposer) selectDstNode() (string, bool) {
	all := p.store.GetAll()
	var lightest string
	var lightestPct float64 = 101 // higher than any valid value
	allOpen := true
	for _, ns := range all {
		if ns.NodeID == p.nodeID {
			continue
		}
		cb, ok := p.cbs[ns.NodeID]
		if !ok || cb.allow() {
			allOpen = false
			if ns.DiskUsedPct < lightestPct {
				lightestPct = ns.DiskUsedPct
				lightest = ns.NodeID
			}
		}
	}
	if allOpen && len(all) > 1 {
		metrics.BalancerCBAllOpenTotal.Inc()
		p.logger.Warn().Msg("balancer: all dst circuit breakers open, skipping migration tick")
		return "", false
	}
	if lightest == "" {
		return "", false
	}
	return lightest, true
}

// SetLeaderLoadTransferEnabled toggles the load-driven leadership transfer (off by
// default). Must be called before Run() starts; the field is read only from the
// actor goroutine. Intended for validation/tests until the behavior is vetted.
func (p *BalancerProposer) SetLeaderLoadTransferEnabled(enabled bool) {
	p.leaderLoadTransferEnabled = enabled
}

// Run starts the balancer actor loop. Blocks until ctx is cancelled or Stop() is called.
// The gossip-interval ticker is hot-reloaded: each tick re-reads
// BalancerGossipInterval() and calls ticker.Reset if it changed. Honors
// BalancerEnabled() — when false, the tick is a no-op (still consumes the tick
// so the interval stays current).
func (p *BalancerProposer) Run(ctx context.Context) {
	// Reset tenure timer here so LeaderTenureMin is measured from the moment this
	// node becomes active (leader), not from when BalancerProposer was constructed.
	p.startedAt = time.Now()
	lastGossipInterval := p.clusterCfg.BalancerGossipInterval()
	ticker := time.NewTicker(lastGossipInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stopCh:
			return
		case msg := <-p.ch:
			p.handleMsg(msg)
		case <-ticker.C:
			if cur := p.clusterCfg.BalancerGossipInterval(); cur != lastGossipInterval {
				ticker.Reset(cur)
				lastGossipInterval = cur
			}
			if !p.clusterCfg.BalancerEnabled() {
				continue
			}
			p.tickOnce()
		}
	}
}

// handleMsg dispatches an inbound actor message. Called from Run() goroutine only.
func (p *BalancerProposer) handleMsg(msg balancerMsg) {
	switch msg.kind {
	case msgBalancerStatus:
		msg.replyCh <- BalancerStatus{
			Active:       p.active.Load(),
			ImbalancePct: imbalancePct(p.store),
			Nodes:        p.store.GetAll(),
		}
	}
}

// Stop signals the actor loop to exit. Idempotent — safe to call multiple times.
func (p *BalancerProposer) Stop() {
	p.stopOnce.Do(func() { close(p.stopCh) })
}

// Active returns whether the balancer is currently in the "active migration"
// hysteresis state. Safe for cross-goroutine reads (atomic).
func (p *BalancerProposer) Active() bool { return p.active.Load() }

// TickCount returns the number of tickOnce evaluations since construction.
// Test-friendly; safe for cross-goroutine reads (atomic).
func (p *BalancerProposer) TickCount() int64 { return p.tickCount.Load() }

// tickOnce is a single balancer evaluation cycle, exposed for testing.
// Must be called from the actor goroutine only. Reads cluster-managed
// tunables fresh from clusterCfg each call (hot-reload).
func (p *BalancerProposer) tickOnce() {
	defer p.tickCount.Add(1)
	if !p.node.IsLeader() {
		return
	}

	peers := p.node.PeerIDs()
	if !p.warmupComplete(peers) {
		return
	}

	triggerPct := p.clusterCfg.BalancerImbalanceTriggerPct()
	stopPct := p.clusterCfg.BalancerImbalanceStopPct()

	// Leader load check: transfer leadership if this leader is significantly
	// overloaded. Gated off by default — see leaderLoadTransferEnabled.
	if p.leaderLoadTransferEnabled && time.Since(p.startedAt) >= p.clusterCfg.BalancerLeaderTenureMin() {
		if _, overloaded := selectPeerByLoad(p.store, p.nodeID, p.leaderLoadThreshold); overloaded {
			if err := p.node.TransferLeadership(); err != nil {
				p.logger.Warn().Err(err).Msg("balancer: TransferLeadership failed")
			} else {
				metrics.BalancerLeaderTransfersTotal.Inc()
			}
			return
		}
	}

	diff := imbalancePct(p.store)
	metrics.BalancerImbalancePct.Set(diff)

	// Effective trigger: relaxed by 1.5× if any node is still within its join grace period.
	effectiveTrigger := triggerPct
	if p.gracePeriod > 0 && p.anyNodeInGracePeriod() {
		effectiveTrigger *= 1.5
		metrics.BalancerGracePeriodActiveTicks.Inc()
	}

	// Sync circuit breakers from latest gossip, then check hysteresis.
	allPeers := p.store.GetAll()
	p.syncCB(allPeers)
	if !p.active.Load() {
		if diff < effectiveTrigger {
			return
		}
		p.active.Store(true)
	} else {
		if diff < stopPct {
			p.active.Store(false)
			return
		}
	}

	if _, ok := p.selectDstNode(); !ok {
		return
	}
	p.logger.Debug().Float64("imbalance_pct", diff).Msg("balancer: shard migration retired; no raft proposal emitted")
}

// BalancerStatus is a point-in-time snapshot of the balancer's state.
type BalancerStatus struct {
	Active       bool               // true when imbalance trigger has fired
	ImbalancePct float64            // current max-min disk usage %
	Nodes        []gossip.NodeStats // all non-expired node stats
}

// Status returns a snapshot of the balancer's current state.
// Blocks until the actor goroutine processes the request.
func (p *BalancerProposer) Status() BalancerStatus {
	replyCh := make(chan BalancerStatus, 1)
	select {
	case p.ch <- balancerMsg{kind: msgBalancerStatus, replyCh: replyCh}:
	case <-p.stopCh:
		return BalancerStatus{}
	}
	select {
	case s := <-replyCh:
		return s
	case <-p.stopCh:
		return BalancerStatus{}
	}
}

// anyNodeInGracePeriod returns true if any node in the store has a non-zero JoinedAt
// within the configured GracePeriod window.
func (p *BalancerProposer) anyNodeInGracePeriod() bool {
	for _, ns := range p.store.GetAll() {
		if !ns.JoinedAt.IsZero() && time.Since(ns.JoinedAt) < p.gracePeriod {
			return true
		}
	}
	return false
}

// warmupComplete returns true once all peers have gossiped recently or the warmup timeout has passed.
// "Recently" is defined as UpdatedAt within peerSeenWindow (or 2× BalancerGossipInterval if zero).
func (p *BalancerProposer) warmupComplete(peers []string) bool {
	if time.Since(p.startedAt) >= p.clusterCfg.BalancerWarmupTimeout() {
		return true
	}
	window := p.peerSeenWindow
	if window == 0 {
		window = 2 * p.clusterCfg.BalancerGossipInterval()
	}
	for _, peerID := range peers {
		ns, ok := p.store.Get(peerID)
		if !ok || time.Since(ns.UpdatedAt) > window {
			return false
		}
	}
	return true
}

// selectLightestPeer returns the nodeID with the lowest DiskUsedPct, excluding self.
//
//nolint:unused // package tests pin peer-selection behaviour.
func selectLightestPeer(store *gossip.NodeStatsStore, selfID string) (string, bool) {
	all := store.GetAll()
	var best string
	bestPct := math.MaxFloat64
	for _, ns := range all {
		if ns.NodeID == selfID {
			continue
		}
		if ns.DiskUsedPct < bestPct {
			bestPct = ns.DiskUsedPct
			best = ns.NodeID
		}
	}
	return best, best != ""
}

// imbalancePct returns max(DiskUsedPct) - min(DiskUsedPct) across all nodes.
func imbalancePct(store *gossip.NodeStatsStore) float64 {
	all := store.GetAll()
	if len(all) < 2 {
		return 0
	}
	lo, hi := math.MaxFloat64, -math.MaxFloat64
	for _, ns := range all {
		if ns.DiskUsedPct < lo {
			lo = ns.DiskUsedPct
		}
		if ns.DiskUsedPct > hi {
			hi = ns.DiskUsedPct
		}
	}
	return hi - lo
}

// selectPeerByLoad returns the peer with the lowest RequestsPerSec when selfID's
// load exceeds median*threshold. Returns ("", false) if no redirect is needed.
func selectPeerByLoad(store *gossip.NodeStatsStore, selfID string, threshold float64) (string, bool) {
	all := store.GetAll()
	if len(all) <= 1 {
		return "", false
	}

	loads := make([]float64, len(all))
	for i, ns := range all {
		loads[i] = ns.RequestsPerSec
	}
	sort.Float64s(loads)
	median := loads[len(loads)/2]

	var selfLoad float64
	for _, ns := range all {
		if ns.NodeID == selfID {
			selfLoad = ns.RequestsPerSec
			break
		}
	}

	if selfLoad <= median*threshold {
		return "", false
	}

	var best string
	bestLoad := math.MaxFloat64
	for _, ns := range all {
		if ns.NodeID == selfID {
			continue
		}
		if ns.RequestsPerSec < bestLoad {
			bestLoad = ns.RequestsPerSec
			best = ns.NodeID
		}
	}
	return best, best != ""
}
