package serveruntime

import (
	"context"
	"runtime/debug"
	"strings"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/server/admin"
	"github.com/prometheus/client_golang/prometheus"
)

const operatorMetricsVersionUnknown = "unknown"

//nolint:unused // operator-state scaffolding landed v0.0.388-389 without a lint gate; production uses metrics.OperatorStateSources. Kept until that feature wires or removes it.
type operatorStateSource struct {
	nodeID   string
	metaRaft metaRaftOperatorSource
	dataNode dataRaftOperatorSource
	peers    admin.PeerHealthAPI
	deps     *admin.Deps
}

type metaRaftOperatorSource interface {
	Node() cluster.RaftNode
	LastApplied() uint64
}

type dataRaftOperatorSource = cluster.RaftNode

func newOperatorStateMetricsCollector(state *bootState) (*metrics.OperatorStateCollector, prometheus.Gatherer) {
	collector := metrics.NewOperatorStateCollector(operatorStateSources(state))
	registry := prometheus.NewRegistry()
	registry.MustRegister(collector)
	return collector, prometheus.Gatherers{prometheus.DefaultGatherer, registry}
}

func operatorStateSources(state *bootState) metrics.OperatorStateSources {
	if state == nil {
		return metrics.OperatorStateSources{}
	}
	return metrics.OperatorStateSources{
		Server:  operatorServerStateSource{nodeID: state.nodeID},
		Cluster: operatorClusterStateSource{nodeID: state.nodeID, metaRaft: state.metaRaft, peers: NewPeerHealthAdapter(state.distBackend)},
		Raft:    operatorRaftStateSource{nodeID: state.nodeID, metaRaft: state.metaRaft, dataNode: state.node},
		Buckets: operatorBucketStateSource{deps: state.adminDeps},
	}
}

type operatorServerStateSource struct {
	nodeID string
}

func (s operatorServerStateSource) ServerStateSnapshot() metrics.OperatorServerState {
	return metrics.OperatorServerState{NodeID: s.nodeID, Version: operatorMetricsVersion()}
}

func operatorMetricsVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return operatorMetricsVersionUnknown
	}
	if info.Main.Version != "" && info.Main.Version != "(devel)" {
		return info.Main.Version
	}
	var revision string
	var modified bool
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			revision = setting.Value
		case "vcs.modified":
			modified = setting.Value == "true"
		}
	}
	if revision == "" {
		return operatorMetricsVersionUnknown
	}
	if len(revision) > 12 {
		revision = revision[:12]
	}
	if modified {
		return strings.TrimSpace(revision) + "-dirty"
	}
	return strings.TrimSpace(revision)
}

type operatorClusterStateSource struct {
	nodeID   string
	metaRaft metaRaftOperatorSource
	peers    admin.PeerHealthAPI
}

func (s operatorClusterStateSource) ClusterStateSnapshot() (metrics.OperatorClusterState, error) {
	counts := map[string]int{}
	node := dataRaftOperatorSource(nil)
	if s.metaRaft != nil {
		node = s.metaRaft.Node()
	}
	if node == nil {
		counts["self_voter"] = 1
		return metrics.OperatorClusterState{MembersByState: counts, QuorumAvailable: true}, nil
	}

	cfg := node.Configuration()
	peerHealth := map[string]bool{}
	if s.peers != nil {
		for _, peer := range s.peers.Snapshot() {
			peerHealth[peer.ID] = peer.Healthy
		}
	}

	voters := 0
	healthyVoters := 0
	for _, server := range cfg.Servers {
		isSelf := server.ID == s.nodeID || server.ID == node.ID()
		healthy := isSelf || peerHealth[server.ID]
		if server.Suffrage == raft.Voter {
			voters++
			if healthy {
				healthyVoters++
			}
			if isSelf {
				counts["self_voter"]++
			} else if healthy {
				counts["healthy_voter_peer"]++
			} else {
				counts["unhealthy_voter_peer"]++
			}
			continue
		}
		if isSelf {
			counts["self_learner"]++
		} else if healthy {
			counts["healthy_learner_peer"]++
		} else {
			counts["unhealthy_learner_peer"]++
		}
	}
	for id := range peerHealth {
		if !serverInConfiguration(cfg, id) {
			counts["unknown_peer"]++
		}
	}
	quorumAvailable := operatorQuorumAvailable(cfg, healthyVoterIDs(cfg, peerHealth, s.nodeID, node.ID()), voters, healthyVoters)
	return metrics.OperatorClusterState{MembersByState: counts, QuorumAvailable: quorumAvailable}, nil
}

func healthyVoterIDs(cfg raft.Configuration, peerHealth map[string]bool, nodeIDs ...string) map[string]bool {
	self := make(map[string]struct{}, len(nodeIDs))
	for _, id := range nodeIDs {
		if id != "" {
			self[id] = struct{}{}
		}
	}
	out := make(map[string]bool)
	for _, server := range cfg.Servers {
		if server.Suffrage != raft.Voter {
			continue
		}
		if _, ok := self[server.ID]; ok || peerHealth[server.ID] {
			out[server.ID] = true
		}
	}
	return out
}

func operatorQuorumAvailable(cfg raft.Configuration, healthy map[string]bool, voters, healthyVoters int) bool {
	if cfg.Joint {
		return majorityHealthy(cfg.OldVoters, healthy) && majorityHealthy(cfg.NewVoters, healthy)
	}
	return voters == 0 || healthyVoters >= voters/2+1
}

func majorityHealthy(voters []string, healthy map[string]bool) bool {
	if len(voters) == 0 {
		return true
	}
	count := 0
	for _, id := range voters {
		if healthy[id] {
			count++
		}
	}
	return count >= len(voters)/2+1
}

func serverInConfiguration(cfg raft.Configuration, id string) bool {
	for _, server := range cfg.Servers {
		if server.ID == id {
			return true
		}
	}
	return false
}

type operatorRaftStateSource struct {
	nodeID   string
	metaRaft metaRaftOperatorSource
	dataNode dataRaftOperatorSource
}

func (s operatorRaftStateSource) RaftStateSnapshot() ([]metrics.OperatorRaftState, error) {
	out := make([]metrics.OperatorRaftState, 0, 2)
	if s.metaRaft != nil && s.metaRaft.Node() != nil {
		node := s.metaRaft.Node()
		out = append(out, raftStateFact(s.nodeID, "meta", node, s.metaRaft.LastApplied(), true))
	}
	if s.dataNode != nil {
		out = append(out, raftStateFact(s.nodeID, "data", s.dataNode, 0, false))
	}
	return out, nil
}

func raftStateFact(nodeID, group string, node dataRaftOperatorSource, applied uint64, hasApplied bool) metrics.OperatorRaftState {
	if nodeID == "" {
		nodeID = node.ID()
	}
	return metrics.OperatorRaftState{
		NodeID:          nodeID,
		Group:           group,
		Role:            raftRoleLabel(node.State()),
		Term:            node.Term(),
		CommitIndex:     node.CommittedIndex(),
		AppliedIndex:    applied,
		HasAppliedIndex: hasApplied,
	}
}

func raftRoleLabel(state raft.NodeState) string {
	switch state {
	case raft.Leader:
		return "leader"
	case raft.Candidate:
		return "candidate"
	case raft.Follower:
		return "follower"
	default:
		return "unknown"
	}
}

type operatorBucketStateSource struct {
	deps *admin.Deps
}

func (s operatorBucketStateSource) BucketStateSnapshot(ctx context.Context) (metrics.OperatorBucketState, error) {
	if s.deps == nil || s.deps.Buckets == nil {
		return metrics.OperatorBucketState{}, nil
	}
	resp, err := admin.AdminListBuckets(ctx, s.deps)
	if err != nil {
		return metrics.OperatorBucketState{}, err
	}
	return metrics.OperatorBucketState{Active: len(resp.Buckets)}, nil
}
