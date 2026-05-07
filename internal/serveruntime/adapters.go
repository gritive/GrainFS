package serveruntime

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/resourcewatch"
	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/server"
	"github.com/gritive/GrainFS/internal/server/admin"
)

// PeerHealthAdapter implements admin.PeerHealthAPI on top of
// *cluster.DistributedBackend. Converts cluster.PeerHealthEntry to the
// JSON-friendly admin.ClusterPeerInfo so the admin handler stays decoupled
// from the cluster package.
type PeerHealthAdapter struct {
	backend *cluster.DistributedBackend
}

// NewPeerHealthAdapter returns a value adapter (no pointer; the struct is
// trivially copyable). Caller passes the freshly-constructed
// *DistributedBackend at admin-deps build time.
func NewPeerHealthAdapter(backend *cluster.DistributedBackend) PeerHealthAdapter {
	return PeerHealthAdapter{backend: backend}
}

func (a PeerHealthAdapter) Snapshot() []admin.ClusterPeerInfo {
	if a.backend == nil {
		return nil
	}
	ph := a.backend.PeerHealth()
	if ph == nil {
		return nil
	}
	src := ph.Snapshot()
	out := make([]admin.ClusterPeerInfo, 0, len(src))
	for _, e := range src {
		info := admin.ClusterPeerInfo{
			ID:                  e.ID,
			Healthy:             e.Healthy,
			CooldownRemainingMs: e.CooldownRemainingMs,
		}
		if e.LastFailure != nil {
			info.LastFailure = e.LastFailure.UTC().Format(time.RFC3339Nano)
		}
		out = append(out, info)
	}
	return out
}

// ReplicaRepairerFunc adapts a function to scrubber.ReplicaRepairer.
type ReplicaRepairerFunc func(ctx context.Context, bucket, key string) error

func (f ReplicaRepairerFunc) RepairReplica(ctx context.Context, bucket, key string) error {
	return f(ctx, bucket, key)
}

// ScrubProposerAdapter implements admin.ScrubProposer over MetaRaft. The
// adapter does a leader-side dedup pre-check so duplicate triggers don't
// consume a fresh raft entry per call.
type ScrubProposerAdapter struct {
	metaRaft *cluster.MetaRaft
	director *scrubber.Director
	nodeID   string
}

func NewScrubProposerAdapter(metaRaft *cluster.MetaRaft, director *scrubber.Director, nodeID string) *ScrubProposerAdapter {
	return &ScrubProposerAdapter{metaRaft: metaRaft, director: director, nodeID: nodeID}
}

func (a *ScrubProposerAdapter) Propose(ctx context.Context, req scrubber.TriggerReq) (scrubber.ScrubTriggerEntry, bool, error) {
	if existing, ok := a.director.LookupDedup(req); ok {
		return existing, false, nil
	}
	entry := scrubber.ScrubTriggerEntry{
		SessionID:        uuid.NewString(),
		Bucket:           req.Bucket,
		KeyPrefix:        req.KeyPrefix,
		Scope:            req.Scope,
		DryRun:           req.DryRun,
		RequestedAt:      time.Now().Unix(),
		OriginatorNodeID: a.nodeID,
	}
	return entry, true, a.metaRaft.ProposeScrubTrigger(ctx, entry)
}

// ScrubAggregatorAdapter implements admin.ScrubAggregator over
// ClusterCoordinator's per-peer fan-out RPC.
type ScrubAggregatorAdapter struct {
	coord *cluster.ClusterCoordinator
}

func NewScrubAggregatorAdapter(coord *cluster.ClusterCoordinator) *ScrubAggregatorAdapter {
	return &ScrubAggregatorAdapter{coord: coord}
}

func (a *ScrubAggregatorAdapter) Peers(ctx context.Context, sessionID string) ([]admin.ScrubJobInfo, []string, error) {
	if a.coord == nil {
		return nil, nil, nil
	}
	stats, failures, err := a.coord.ScrubSessionStat(ctx, sessionID)
	if err != nil {
		return nil, nil, err
	}
	infos := make([]admin.ScrubJobInfo, 0, len(stats))
	for _, s := range stats {
		scope := "full"
		if s.Scope == int32(scrubber.ScopeLive) {
			scope = "live"
		}
		infos = append(infos, admin.ScrubJobInfo{
			Bucket:       s.Bucket,
			KeyPrefix:    s.KeyPrefix,
			Scope:        scope,
			DryRun:       s.DryRun,
			Status:       s.Status,
			StartedAt:    s.StartedAt,
			DoneAt:       s.DoneAt,
			Checked:      s.Checked,
			Healthy:      s.Healthy,
			Detected:     s.Detected,
			Repaired:     s.Repaired,
			Unrepairable: s.Unrepairable,
			Skipped:      s.Skipped,
			OwnedHere:    s.OwnedHere,
		})
	}
	return infos, failures, nil
}

// VlogBreakdownOptions is the cobra-free input struct for the
// `GET /v1/resource/vlog/breakdown` endpoint adapter.
type VlogBreakdownOptions struct {
	Enabled       bool
	DataDir       string
	WarnRatio     float64
	CriticalRatio float64
}

// VlogBreakdownAdapter implements admin.VlogBreakdownAPI. It re-runs
// registry smoke on every call (operator-initiated, low QPS) so callers
// see fresh stale/live state rather than the stale 60s-startup snapshot.
type VlogBreakdownAdapter struct {
	registry *resourcewatch.Registry
	provider *resourcewatch.VlogProvider
	dataDir  string
	warn     float64
	critical float64
}

// NewVlogBreakdownAdapter returns nil when opts.Enabled is false so the
// admin Deps wiring can pass the result through unconditionally; caller
// treats nil as "endpoint disabled".
func NewVlogBreakdownAdapter(opts VlogBreakdownOptions) admin.VlogBreakdownAPI {
	if !opts.Enabled {
		return nil
	}
	return &VlogBreakdownAdapter{
		registry: resourcewatch.Default,
		provider: resourcewatch.NewVlogProvider(resourcewatch.VlogProviderOptions{DataDir: opts.DataDir}),
		dataDir:  opts.DataDir,
		warn:     opts.WarnRatio,
		critical: opts.CriticalRatio,
	}
}

func (a *VlogBreakdownAdapter) Breakdown() (admin.VlogBreakdownResp, error) {
	sample, err := a.provider.Snapshot(context.Background())
	if err != nil {
		return admin.VlogBreakdownResp{}, fmt.Errorf("vlog snapshot: %w", err)
	}
	cats := make([]admin.VlogCategoryBytes, 0, len(sample.Categories))
	for k, v := range sample.Categories {
		cats = append(cats, admin.VlogCategoryBytes{Category: string(k), VlogBytes: int64(v)})
	}
	sort.Slice(cats, func(i, j int) bool { return cats[i].VlogBytes > cats[j].VlogBytes })

	gcFails := make(map[string]int32)
	for _, e := range a.registry.Snapshot() {
		gcFails[string(e.Category)] = e.ConsecutiveGCFailures()
	}

	smoke, _ := resourcewatch.VerifyVlogRegistry(a.dataDir, a.registry, false)
	live, stale := smoke.Live, smoke.Stale
	if live == nil {
		live = []string{}
	}
	if stale == nil {
		stale = []string{}
	}

	var ratio float64
	if sample.Limit > 0 {
		ratio = float64(sample.Open) / float64(sample.Limit)
	}
	level := "ok"
	switch {
	case ratio >= a.critical:
		level = "critical"
	case ratio >= a.warn:
		level = "warn"
	}
	return admin.VlogBreakdownResp{
		TotalVlogBytes: int64(sample.Open),
		LimitBytes:     int64(sample.Limit),
		Ratio:          ratio,
		Level:          level,
		Categories:     cats,
		GCFailures:     gcFails,
		SmokeReport:    admin.VlogSmokeReport{Live: live, Stale: stale},
	}, nil
}

// BalancerInfoAdapter adapts *cluster.BalancerProposer to server.BalancerInfo.
type BalancerInfoAdapter struct {
	p *cluster.BalancerProposer
}

func NewBalancerInfoAdapter(p *cluster.BalancerProposer) *BalancerInfoAdapter {
	return &BalancerInfoAdapter{p: p}
}

func (a *BalancerInfoAdapter) Status() server.BalancerStatusResult {
	st := a.p.Status()
	nodes := make([]server.BalancerNodeInfo, len(st.Nodes))
	for i, n := range st.Nodes {
		nodes[i] = server.BalancerNodeInfo{
			NodeID:         n.NodeID,
			DiskUsedPct:    n.DiskUsedPct,
			DiskAvailBytes: n.DiskAvailBytes,
			RequestsPerSec: n.RequestsPerSec,
			JoinedAt:       n.JoinedAt,
			UpdatedAt:      n.UpdatedAt,
		}
	}
	return server.BalancerStatusResult{
		Active:       st.Active,
		ImbalancePct: st.ImbalancePct,
		Nodes:        nodes,
	}
}

// RaftClusterInfo adapts raft.Node to server.ClusterInfo. addrBook
// resolves stable node IDs from raft peer aliases (PR-D peer-identity
// unification).
type RaftClusterInfo struct {
	node     *raft.Node
	peers    []string
	backend  *cluster.DistributedBackend
	addrBook cluster.NodeAddressBook
}

type peerReplicationEvidenceSource interface {
	PeerReplicationEvidence() []raft.PeerReplicationEvidence
}

func NewRaftClusterInfo(node *raft.Node, peers []string, backend *cluster.DistributedBackend, addrBook cluster.NodeAddressBook) *RaftClusterInfo {
	return &RaftClusterInfo{node: node, peers: peers, backend: backend, addrBook: addrBook}
}

func (r *RaftClusterInfo) NodeID() string   { return r.node.ID() }
func (r *RaftClusterInfo) State() string    { return r.node.State().String() }
func (r *RaftClusterInfo) Term() uint64     { return r.node.Term() }
func (r *RaftClusterInfo) LeaderID() string { return r.node.LeaderID() }
func (r *RaftClusterInfo) Peers() []string {
	return nilToEmpty(r.normalizePeerIDs(r.node.Peers()))
}

// LivePeers reports all metaRaft voters as live: self plus every remote.
// PR-D unifies peer identity so the fallback no longer mixes node IDs
// and raft addresses. Fine-grained liveness remains a later peer-health
// signal.
func (r *RaftClusterInfo) LivePeers() []string {
	peers := r.normalizePeerIDs(r.node.Peers())
	out := make([]string, 0, len(peers)+1)
	if id := r.node.ID(); id != "" {
		out = append(out, id)
	}
	out = append(out, peers...)
	return out
}

func (r *RaftClusterInfo) PeerAddrs() map[string]string {
	out := make(map[string]string)
	if r.addrBook == nil {
		return out
	}
	for _, peer := range r.node.Peers() {
		resolved := cluster.ResolveShardGroupPeer(r.addrBook, peer)
		if resolved.NodeID != "" && resolved.RaftAddr != "" {
			out[resolved.NodeID] = resolved.RaftAddr
		}
	}
	return out
}

func (r *RaftClusterInfo) PeerStates() map[string]string {
	out := make(map[string]string)
	for _, peer := range r.node.Peers() {
		resolved := cluster.ResolveShardGroupPeer(r.addrBook, peer)
		id := resolved.NodeID
		if id == "" {
			id = peer
		}
		if resolved.Unresolved {
			out[id] = "unresolved_legacy"
			continue
		}
		out[id] = "configured"
	}
	return out
}

func (r *RaftClusterInfo) PeerSnapshot() []cluster.PeerLivenessRow {
	var evidence []raft.PeerReplicationEvidence
	if source, ok := any(r.node).(peerReplicationEvidenceSource); ok {
		evidence = source.PeerReplicationEvidence()
	}
	return cluster.BuildPeerLivenessSnapshot(cluster.PeerLivenessInput{
		SelfID:       r.node.ID(),
		Voters:       r.node.Peers(),
		AddressBook:  r.addrBook,
		ProbeResults: freshReplicationProbeResults(evidence, r.addrBook, time.Now(), cluster.MetaRaftLivenessFreshnessWindow),
	})
}

func freshReplicationProbeResults(evidence []raft.PeerReplicationEvidence, addrBook cluster.NodeAddressBook, now time.Time, freshness time.Duration) []cluster.PeerProbeResult {
	if freshness <= 0 {
		return nil
	}
	out := make([]cluster.PeerProbeResult, 0, len(evidence))
	for _, e := range evidence {
		if e.PeerID == "" || e.LastAppendSuccess.IsZero() {
			continue
		}
		if now.Sub(e.LastAppendSuccess) > freshness {
			continue
		}
		peerID := e.PeerID
		if resolved := cluster.ResolveShardGroupPeer(addrBook, e.PeerID); resolved.NodeID != "" {
			peerID = resolved.NodeID
		}
		out = append(out, cluster.PeerProbeResult{
			PeerID:     peerID,
			Live:       true,
			ObservedAt: e.LastAppendSuccess,
			Reason:     "raft_append_success",
		})
	}
	return out
}

func (r *RaftClusterInfo) BucketAssignments() map[string]string {
	src, ok := r.addrBook.(interface {
		BucketAssignments() map[string]string
	})
	if !ok {
		return nil
	}
	return src.BucketAssignments()
}

func (r *RaftClusterInfo) ShardGroups() []cluster.ShardGroupEntry {
	src, ok := r.addrBook.(interface {
		ShardGroups() []cluster.ShardGroupEntry
	})
	if !ok {
		return nil
	}
	return src.ShardGroups()
}

func (r *RaftClusterInfo) ObjectIndexSummary(bucket string) cluster.ObjectIndexSummary {
	src, ok := r.addrBook.(interface {
		ObjectIndexSummary(bucket string) cluster.ObjectIndexSummary
	})
	if !ok {
		return cluster.ObjectIndexSummary{Bucket: bucket, PlacementGroupCounts: map[string]int{}}
	}
	return src.ObjectIndexSummary(bucket)
}

func (r *RaftClusterInfo) PlacementReport(bucket, key string, maxRows int) cluster.PlacementReport {
	src, ok := r.addrBook.(interface {
		PlacementReport(bucket, key string, maxRows int) cluster.PlacementReport
	})
	if !ok {
		return cluster.PlacementReport{
			DesiredPolicyBasis:  "group_voter_count",
			Bucket:              bucket,
			Key:                 key,
			ActualProfileCounts: map[string]int{},
		}
	}
	return src.PlacementReport(bucket, key, maxRows)
}

func (r *RaftClusterInfo) normalizePeerIDs(peers []string) []string {
	if len(peers) == 0 {
		return nil
	}
	if r.addrBook == nil {
		out := make([]string, len(peers))
		copy(out, peers)
		return out
	}
	out := make([]string, len(peers))
	for i, peer := range peers {
		out[i] = cluster.ResolveShardGroupPeer(r.addrBook, peer).NodeID
		if out[i] == "" {
			out[i] = peer
		}
	}
	return out
}

// nilToEmpty normalises a nil slice to an empty one so JSON marshals it as
// "[]" instead of "null".
func nilToEmpty(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// RaftMembership adapts raft.Node to server.ClusterMembership for the
// remove-peer endpoint. Joint consensus (§4.3) is used so the change is
// atomic and the engine handles leader self-removal via commit-time
// wakeup.
type RaftMembership struct{ node *raft.Node }

func NewRaftMembership(node *raft.Node) *RaftMembership {
	return &RaftMembership{node: node}
}

func (r *RaftMembership) RemoveVoter(ctx context.Context, id string) error {
	return r.node.ChangeMembership(ctx, nil, []string{id})
}
