package cluster

import (
	"context"
	"log/slog"
	"math"
	"sort"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

// BalancerConfig holds tunable parameters for the BalancerProposer.
// All fields can be injected at construction time; tests use small values to speed up loops.
type BalancerConfig struct {
	GossipInterval      time.Duration
	WarmupTimeout       time.Duration
	ImbalanceTriggerPct float64 // start migration when max-min disk diff exceeds this
	ImbalanceStopPct    float64 // stop migration when max-min disk diff drops below this
	MigrationRate       int     // max proposals per tick (reserved for rate limiting)
	LeaderTenureMin     time.Duration
	LeaderLoadThreshold float64 // leader's requestsPerSec / median before transfer
}

// DefaultBalancerConfig returns production-safe defaults.
func DefaultBalancerConfig() BalancerConfig {
	return BalancerConfig{
		GossipInterval:      30 * time.Second,
		WarmupTimeout:       60 * time.Second,
		ImbalanceTriggerPct: 20.0,
		ImbalanceStopPct:    5.0,
		MigrationRate:       1,
		LeaderTenureMin:     5 * time.Minute,
		LeaderLoadThreshold: 1.3,
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

// BalancerProposer monitors NodeStatsStore and proposes CmdMigrateShard when
// disk usage is imbalanced across nodes. Only the Raft leader runs proposals.
type BalancerProposer struct {
	nodeID   string
	store    *NodeStatsStore
	node     RaftBalancerNode
	cfg      BalancerConfig
	active   bool // hysteresis state: true once trigger fired, false after stop threshold
	startedAt time.Time
	logger   *slog.Logger
}

// NewBalancerProposer creates a BalancerProposer with the given config.
func NewBalancerProposer(nodeID string, store *NodeStatsStore, node RaftBalancerNode, cfg BalancerConfig) *BalancerProposer {
	return &BalancerProposer{
		nodeID:    nodeID,
		store:     store,
		node:      node,
		cfg:       cfg,
		startedAt: time.Now(),
		logger:    slog.Default().With("component", "balancer"),
	}
}

// Run starts the balancer tick loop. Blocks until ctx is cancelled.
func (p *BalancerProposer) Run(ctx context.Context) {
	ticker := time.NewTicker(p.cfg.GossipInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.tickOnce(ctx)
		}
	}
}

// tickOnce is a single balancer evaluation cycle, exposed for testing.
func (p *BalancerProposer) tickOnce(ctx context.Context) {
	if !p.node.IsLeader() {
		return
	}

	peers := p.node.PeerIDs()
	if !p.warmupComplete(peers) {
		return
	}

	// Leader load check: transfer leadership if this leader is significantly overloaded.
	if time.Since(p.startedAt) >= p.cfg.LeaderTenureMin {
		if _, overloaded := selectPeerByLoad(p.store, p.nodeID, p.cfg.LeaderLoadThreshold); overloaded {
			if err := p.node.TransferLeadership(); err != nil {
				p.logger.Warn("balancer: TransferLeadership failed", "err", err)
			}
			return
		}
	}

	diff := imbalancePct(p.store)

	// Hysteresis: activate above trigger, deactivate below stop.
	if !p.active {
		if diff < p.cfg.ImbalanceTriggerPct {
			return
		}
		p.active = true
	} else {
		if diff < p.cfg.ImbalanceStopPct {
			p.active = false
			return
		}
	}

	dst, ok := selectLightestPeer(p.store, p.nodeID)
	if !ok {
		return
	}

	p.proposeMigration(ctx, p.nodeID, dst)
}

// warmupComplete returns true once all peers have gossiped or the warmup timeout has passed.
func (p *BalancerProposer) warmupComplete(peers []string) bool {
	if time.Since(p.startedAt) >= p.cfg.WarmupTimeout {
		return true
	}
	// +1 for self
	return p.store.Len() >= len(peers)+1
}

// proposeMigration encodes and proposes a CmdMigrateShard for a single object.
// In full implementation this would iterate over objects on src; here we emit one
// placeholder proposal (the migration executor resolves actual objects).
func (p *BalancerProposer) proposeMigration(ctx context.Context, src, dst string) {
	inner, err := proto.Marshal(&clusterpb.MigrateShardCmd{
		SrcNode: src,
		DstNode: dst,
	})
	if err != nil {
		p.logger.Error("balancer: marshal MigrateShardCmd", "err", err)
		return
	}
	outer, err := proto.Marshal(&clusterpb.Command{
		Type: uint32(CmdMigrateShard),
		Data: inner,
	})
	if err != nil {
		p.logger.Error("balancer: marshal Command", "err", err)
		return
	}
	if err := p.node.Propose(outer); err != nil {
		p.logger.Warn("balancer: propose failed", "src", src, "dst", dst, "err", err)
	}
}

// selectLightestPeer returns the nodeID with the lowest DiskUsedPct, excluding self.
func selectLightestPeer(store *NodeStatsStore, selfID string) (string, bool) {
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
func imbalancePct(store *NodeStatsStore) float64 {
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
func selectPeerByLoad(store *NodeStatsStore, selfID string, threshold float64) (string, bool) {
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
