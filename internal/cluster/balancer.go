package cluster

import (
	"context"
	"io/fs"
	"log/slog"
	"math"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/metrics"
)

// migrationInflightTTL is the duration a proposed migration is tracked to prevent
// re-proposing the same object while Phase 1-3 are in progress.
const migrationInflightTTL = 5 * time.Minute

// ObjectPicker selects an object stored locally on the source node for migration.
// SrcNode is always the leader itself, so implementations scan local storage.
type ObjectPicker interface {
	// PickObjectOnSrcNode returns a (bucket, key, versionID, ok) tuple identifying
	// one locally-stored object suitable for migration. skipIDs contains inflight
	// migration IDs (bucket/key/versionID) to skip. Returns ok=false if none found.
	PickObjectOnSrcNode(nodeID string, skipIDs map[string]bool) (bucket, key, versionID string, ok bool)
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

// PickObjectOnSrcNode returns the first locally-stored object that has a shard_0 file
// and is not present in skipIDs (inflight migrations). nodeID is accepted for interface
// compatibility but ignored (always scans local dir).
// Uses WalkDir to handle S3 keys containing '/' — ShardService stores them verbatim as
// nested directories (e.g. key "a/b/c" → {bucket}/a/b/c/shard_0), so a 2-level ReadDir
// would miss them. versionID is always "" because ShardService is version-oblivious.
func (p *LocalObjectPicker) PickObjectOnSrcNode(_ string, skipIDs map[string]bool) (string, string, string, bool) {
	var foundBucket, foundKey string
	found := false

	_ = filepath.WalkDir(p.shardsDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			slog.Warn("LocalObjectPicker: WalkDir error", "path", path, "err", err)
			return nil
		}
		if found {
			return nil
		}
		if d.IsDir() || d.Name() != "shard_0" {
			return nil
		}
		rel, relErr := filepath.Rel(p.shardsDir, path)
		if relErr != nil {
			return nil
		}
		// rel = "{bucket}/{key...}/shard_0"
		parts := strings.SplitN(rel, string(filepath.Separator), 2)
		if len(parts) != 2 {
			return nil
		}
		bucket := parts[0]
		key := filepath.Dir(parts[1]) // strip trailing "/shard_0"
		if skipIDs[bucket+"/"+key+"/"] {
			return nil // already inflight, keep walking
		}
		foundBucket = bucket
		foundKey = key
		found = true
		return fs.SkipAll
	})

	if found {
		return foundBucket, foundKey, "", true
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
	// GracePeriod is how long after a node joins before it's counted toward the normal
	// imbalance trigger. During this window the trigger is relaxed by 1.5× to prevent
	// migration storms caused by newly-added nodes.
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
		GracePeriod:         10 * time.Minute,
		PeerSeenWindow:      60 * time.Second, // 2× default GossipInterval
		CBThreshold:         0.90,
		MigrationMaxRetries: 3,
		MigrationPendingTTL: 5 * time.Minute,
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
	mu        sync.Mutex           // protects active, inflight; cbs written by syncCB (ticker goroutine only)
	active    bool                 // hysteresis state: true once trigger fired, false after stop threshold
	startedAt time.Time
	picker    ObjectPicker         // nil = no proposals until SetObjectPicker is called
	inflight  map[string]time.Time // proposed migrations not yet committed; keyed by task.id()
	cbs       map[string]*circuitBreaker // per-peer CBs; keyed by peer nodeID
	logger    *slog.Logger
}

// NewBalancerProposer creates a BalancerProposer with the given config.
func NewBalancerProposer(nodeID string, store *NodeStatsStore, node RaftBalancerNode, cfg BalancerConfig) *BalancerProposer {
	if cfg.CBThreshold == 0 {
		cfg.CBThreshold = DefaultBalancerConfig().CBThreshold
	}
	return &BalancerProposer{
		nodeID:    nodeID,
		store:     store,
		node:      node,
		cfg:       cfg,
		startedAt: time.Now(),
		inflight:  make(map[string]time.Time),
		cbs:       make(map[string]*circuitBreaker),
		logger:    slog.Default().With("component", "balancer"),
	}
}

// syncCB updates (or creates) per-peer circuit breakers from the latest gossip stats.
// Must be called with mu held.
func (p *BalancerProposer) syncCB(peers []NodeStats) {
	for _, ns := range peers {
		if ns.NodeID == p.nodeID {
			continue // skip self
		}
		cb, ok := p.cbs[ns.NodeID]
		if !ok {
			cb = newCircuitBreaker(p.cfg.CBThreshold)
			p.cbs[ns.NodeID] = cb
		}
		cb.update(ns)
		if !cb.allow() {
			metrics.BalancerCBOpen.WithLabelValues(ns.NodeID).Set(1)
		} else {
			metrics.BalancerCBOpen.WithLabelValues(ns.NodeID).Set(0)
		}
	}
}

// getCB returns the circuitBreaker for nodeID, or nil if not found.
// Used for testing only.
func (p *BalancerProposer) getCB(nodeID string) *circuitBreaker {
	p.mu.Lock()
	cb := p.cbs[nodeID]
	p.mu.Unlock()
	return cb
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
		p.logger.Warn("balancer: all dst circuit breakers open, skipping migration tick")
		return "", false
	}
	if lightest == "" {
		return "", false
	}
	return lightest, true
}

// NotifyMigrationDone removes the inflight entry for the given migration, allowing
// the same object to be re-proposed if it still exists on this node.
// Called by the FSM when CmdMigrationDone is applied (FSM goroutine).
func (p *BalancerProposer) NotifyMigrationDone(bucket, key, versionID string) {
	id := bucket + "/" + key + "/" + versionID
	p.mu.Lock()
	delete(p.inflight, id)
	p.mu.Unlock()
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
			p.tickOnce()
		}
	}
}

// tickOnce is a single balancer evaluation cycle, exposed for testing.
func (p *BalancerProposer) tickOnce() {
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
			} else {
				metrics.BalancerLeaderTransfersTotal.Inc()
			}
			return
		}
	}

	diff := imbalancePct(p.store)
	metrics.BalancerImbalancePct.Set(diff)

	// Effective trigger: relaxed by 1.5× if any node is still within its join grace period.
	effectiveTrigger := p.cfg.ImbalanceTriggerPct
	if p.cfg.GracePeriod > 0 && p.anyNodeInGracePeriod() {
		effectiveTrigger *= 1.5
		metrics.BalancerGracePeriodActiveTicks.Inc()
	}

	// Sync circuit breakers from latest gossip, then check hysteresis.
	allPeers := p.store.GetAll()
	p.mu.Lock()
	p.syncCB(allPeers)
	active := p.active
	if !active {
		if diff < effectiveTrigger {
			p.mu.Unlock()
			return
		}
		p.active = true
	} else {
		if diff < p.cfg.ImbalanceStopPct {
			p.active = false
			p.mu.Unlock()
			return
		}
	}
	p.mu.Unlock()

	dst, ok := p.selectDstNode()
	if !ok {
		return
	}

	p.proposeMigration(p.nodeID, dst)
}

// BalancerStatus is a point-in-time snapshot of the balancer's state.
type BalancerStatus struct {
	Active       bool        // true when imbalance trigger has fired
	ImbalancePct float64     // current max-min disk usage %
	Nodes        []NodeStats // all non-expired node stats
}

// Status returns a snapshot of the balancer's current state.
func (p *BalancerProposer) Status() BalancerStatus {
	p.mu.Lock()
	active := p.active
	p.mu.Unlock()
	return BalancerStatus{
		Active:       active,
		ImbalancePct: imbalancePct(p.store),
		Nodes:        p.store.GetAll(),
	}
}

// anyNodeInGracePeriod returns true if any node in the store has a non-zero JoinedAt
// within the configured GracePeriod window.
func (p *BalancerProposer) anyNodeInGracePeriod() bool {
	for _, ns := range p.store.GetAll() {
		if !ns.JoinedAt.IsZero() && time.Since(ns.JoinedAt) < p.cfg.GracePeriod {
			return true
		}
	}
	return false
}

// warmupComplete returns true once all peers have gossiped recently or the warmup timeout has passed.
// "Recently" is defined as UpdatedAt within PeerSeenWindow (or 2× GossipInterval if zero).
func (p *BalancerProposer) warmupComplete(peers []string) bool {
	if time.Since(p.startedAt) >= p.cfg.WarmupTimeout {
		return true
	}
	window := p.cfg.PeerSeenWindow
	if window == 0 {
		window = 2 * p.cfg.GossipInterval
	}
	for _, peerID := range peers {
		ns, ok := p.store.Get(peerID)
		if !ok || time.Since(ns.UpdatedAt) > window {
			return false
		}
	}
	return true
}

// proposeMigration selects one object from src via the ObjectPicker and proposes
// a CmdMigrateShard to Raft. Returns early if picker is nil or returns ok=false.
func (p *BalancerProposer) proposeMigration(src, dst string) {
	if p.picker == nil {
		return
	}

	// Sweep expired inflight entries and build skip set under the lock.
	now := time.Now()
	p.mu.Lock()
	for k, exp := range p.inflight {
		if now.After(exp) {
			delete(p.inflight, k)
		}
	}
	skipIDs := make(map[string]bool, len(p.inflight))
	for k := range p.inflight {
		skipIDs[k] = true
	}
	p.mu.Unlock()

	bucket, key, versionID, ok := p.picker.PickObjectOnSrcNode(src, skipIDs)
	if !ok {
		return
	}

	// Guard: picker may not respect skipIDs (e.g., mock in tests). Double-check.
	inflightID := bucket + "/" + key + "/" + versionID
	p.mu.Lock()
	if exp, inFlight := p.inflight[inflightID]; inFlight && now.Before(exp) {
		p.mu.Unlock()
		return
	}
	p.inflight[inflightID] = now.Add(migrationInflightTTL)
	p.mu.Unlock()

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
		return
	}
	metrics.BalancerMigrationsProposedTotal.Inc()
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
