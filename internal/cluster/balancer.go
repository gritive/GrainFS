package cluster

import (
	"context"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sort"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

// ObjectPicker selects an object stored locally on the source node for migration.
// SrcNode is always the leader itself, so implementations scan local storage.
type ObjectPicker interface {
	// PickObjectOnSrcNode returns a (bucket, key, versionID, ok) tuple identifying
	// one locally-stored object suitable for migration. Returns ok=false if none found.
	PickObjectOnSrcNode(nodeID string) (bucket, key, versionID string, ok bool)
}

// LocalObjectPicker scans the local shard directory (shardsDir/{bucket}/{key}/shard_0)
// to find objects stored on this node. This is correct because BadgerDB obj: metadata
// is Raft-replicated to every node — scanning it would return cluster-wide objects,
// not locally-stored ones.
type LocalObjectPicker struct {
	shardsDir string
}

// NewLocalObjectPicker creates a picker that scans shardsDir for locally-stored objects.
// shardsDir should be the directory passed to ShardService (typically dataDir/shards).
func NewLocalObjectPicker(shardsDir string) *LocalObjectPicker {
	return &LocalObjectPicker{shardsDir: shardsDir}
}

// PickObjectOnSrcNode returns the first object that has shard_0 stored locally.
// nodeID is accepted for interface compatibility but ignored (always scans local dir).
func (p *LocalObjectPicker) PickObjectOnSrcNode(_ string) (string, string, string, bool) {
	buckets, err := os.ReadDir(p.shardsDir)
	if err != nil {
		return "", "", "", false
	}
	for _, b := range buckets {
		if !b.IsDir() {
			continue
		}
		keys, err := os.ReadDir(filepath.Join(p.shardsDir, b.Name()))
		if err != nil {
			continue
		}
		for _, k := range keys {
			if !k.IsDir() {
				continue
			}
			shard0 := filepath.Join(p.shardsDir, b.Name(), k.Name(), "shard_0")
			if _, err := os.Stat(shard0); err == nil {
				return b.Name(), k.Name(), "", true
			}
		}
	}
	return "", "", "", false
}

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
	nodeID    string
	store     *NodeStatsStore
	node      RaftBalancerNode
	cfg       BalancerConfig
	active    bool        // hysteresis state: true once trigger fired, false after stop threshold
	startedAt time.Time
	picker    ObjectPicker // nil = no proposals until SetObjectPicker is called
	logger    *slog.Logger
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

// SetObjectPicker sets the picker used by proposeMigration to select which object to move.
// Must be called before Run; if never called, no migration proposals are emitted.
func (p *BalancerProposer) SetObjectPicker(picker ObjectPicker) {
	p.picker = picker
}

// Run starts the balancer tick loop. Blocks until ctx is cancelled.
func (p *BalancerProposer) Run(ctx context.Context) {
	// Reset tenure timer here so LeaderTenureMin is measured from the moment this
	// node becomes active (leader), not from when BalancerProposer was constructed.
	p.startedAt = time.Now()
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

// proposeMigration selects one object from src via the ObjectPicker and proposes
// a CmdMigrateShard to Raft. Returns early if picker is nil or returns ok=false.
func (p *BalancerProposer) proposeMigration(ctx context.Context, src, dst string) {
	if p.picker == nil {
		return
	}
	bucket, key, versionID, ok := p.picker.PickObjectOnSrcNode(src)
	if !ok {
		return
	}
	inner, err := proto.Marshal(&clusterpb.MigrateShardCmd{
		Bucket:    bucket,
		Key:       key,
		VersionId: versionID,
		SrcNode:   src,
		DstNode:   dst,
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
