package serveruntime

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/gossip"
	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/raft"
)

// RaftBalancerAdapter wraps cluster.RaftNode to implement
// cluster.RaftBalancerNode. Used internally by StartBalancer; exported so
// tests outside this package can supply their own adapter when wiring a
// fake balancer.
//
// The node field accepts both v1 (*raft.Node) and v2 (*raftV2Node) through
// the cluster.RaftNode interface so M5 PR 26 can swap implementations
// behind GRAINFS_RAFT_V2=serveruntime without touching the balancer.
type RaftBalancerAdapter struct {
	node  cluster.RaftNode
	peers []string
}

func NewRaftBalancerAdapter(node cluster.RaftNode, peers []string) *RaftBalancerAdapter {
	return &RaftBalancerAdapter{node: node, peers: peers}
}

func (a *RaftBalancerAdapter) Propose(data []byte) error { return a.node.Propose(data) }
func (a *RaftBalancerAdapter) IsLeader() bool            { return a.node.State() == raft.Leader }
func (a *RaftBalancerAdapter) NodeID() string            { return a.node.ID() }
func (a *RaftBalancerAdapter) PeerIDs() []string         { return a.peers }
func (a *RaftBalancerAdapter) TransferLeadership() error { return a.node.TransferLeadership() }

// StartBalancer wires and launches the BalancerProposer, GossipSender,
// GossipReceiver, MigrationExecutor and migration task channel, then
// replays any persisted pending tasks. Returns the GossipReceiver so the
// caller can wire additional StreamType consumers (e.g. Phase 16 Slice 2
// receipt gossip) onto the same receiver.
//
// clusterCfg is the live ClusterConfig view (typically metaFSM.ClusterConfig())
// — the balancer reads its 10 cluster-managed tunables from it every tick.
// diskCfg is the same view narrowed to disk thresholds for the standalone
// DiskCollector spun up here; pass the same *ClusterConfig as clusterCfg.
func StartBalancer(
	ctx context.Context,
	nodeID, dataDir string,
	statsStore *gossip.NodeStatsStore,
	node cluster.RaftNode,
	peers []string,
	fsm *cluster.FSM,
	clusterTransport gossip.GossipTransport,
	shardSvc *cluster.ShardService,
	numShards int,
	clusterCfg cluster.BalancerClusterCfg,
	diskCfg cluster.DiskCfgReader,
	capabilityGate *cluster.CapabilityGate,
	capabilityEvidence gossip.CapabilityEvidenceSource,
	addrBook cluster.NodeAddressBook,
	gossipPeerProvider func() []string,
) (*cluster.BalancerProposer, *gossip.GossipReceiver, error) {
	gossipInterval := clusterCfg.BalancerGossipInterval()
	migrationPendingTTL := clusterCfg.BalancerMigrationPendingTTL()
	migrationMaxRetries := int(clusterCfg.BalancerMigrationMaxRetries())

	adapter := NewRaftBalancerAdapter(node, peers)
	balancer := cluster.NewBalancerProposer(nodeID, statsStore, adapter, clusterCfg)

	balancer.SetObjectPicker(cluster.NewLocalObjectPicker(filepath.Join(dataDir, "shards")))

	taskCh := make(chan cluster.MigrationTask, 256)

	exec := cluster.NewMigrationExecutorWithTTL(shardSvc, adapter, numShards, migrationPendingTTL)
	if migrationMaxRetries > 0 {
		exec.SetMaxWriteRetries(migrationMaxRetries)
	}
	exec.SetShardCounter(ECShardCounterFor(fsm))
	exec.Start(ctx)

	fsm.SetMigrationHooks(taskCh, exec, balancer)

	capabilityEvidenceAliasProvider := func() []string {
		if addrBook == nil {
			return nil
		}
		addr, ok := cluster.ResolveNodeAddress(addrBook, nodeID)
		if !ok || addr == "" || addr == nodeID {
			return nil
		}
		return []string{addr}
	}
	sender := gossip.NewGossipSender(nodeID, peers, clusterTransport, statsStore, gossipInterval).
		WithPeerProvider(gossipPeerProvider).
		WithCapabilityEvidenceSource(capabilityEvidence).
		WithCapabilityGate(capabilityGate).
		WithCapabilityEvidenceAliasProvider(capabilityEvidenceAliasProvider)
	receiver := gossip.NewGossipReceiver(clusterTransport, statsStore).
		WithCapabilityGate(capabilityGate).
		WithAddressResolver(cluster.NodeAddressBookResolver(addrBook))

	go sender.Run(ctx)
	// Phase 8 N7-3: the receiver consumes the native /gossip/admin +
	// /gossip/receipt routes (the transport's per-route drain goroutines
	// replace the Receive()-loop goroutine).
	receiver.RegisterNativeGossipRoutes()
	go exec.Run(ctx, taskCh)
	go balancer.Run(ctx)

	statsStore.Set(gossip.NodeStats{
		NodeID:   nodeID,
		JoinedAt: time.Now(),
	})

	var dirs []string
	if shardSvc != nil {
		dirs = shardSvc.DataDirs()
	} else {
		dirs = []string{dataDir}
	}
	collector := cluster.NewMultiRootDiskCollector(nodeID, dirs, statsStore, gossipInterval, diskCfg)
	if testPctStr := os.Getenv("GRAINFS_TEST_DISK_PCT"); testPctStr != "" {
		var testPct float64
		if _, err := fmt.Sscanf(testPctStr, "%f", &testPct); err != nil {
			return nil, nil, fmt.Errorf("GRAINFS_TEST_DISK_PCT: invalid value %q: %w", testPctStr, err)
		}
		if testPct < 0 || testPct > 100 {
			return nil, nil, fmt.Errorf("GRAINFS_TEST_DISK_PCT: value %v out of range [0,100]", testPct)
		}
		log.Warn().Float64("pct", testPct).Msg("GRAINFS_TEST_DISK_PCT active — real disk stats overridden")
		collector.SetStatFunc(func(string) (float64, uint64) { return testPct, 0 })
	}
	go collector.Run(ctx)

	// Request-rate collector: samples the local service-request counter off the
	// hot path and writes RequestsPerSec into the stats store, completing the
	// gossip → BoundedLoads/balancer load-signal supply chain (Phase 6 S6-2).
	rpsCollector := cluster.NewRequestRateCollector(nodeID, statsStore, gossipInterval, metrics.ServiceRequestCount)
	go rpsCollector.Run(ctx)

	if err := fsm.RecoverPending(ctx, taskCh); err != nil {
		log.Warn().Err(err).Msg("balancer: recover pending failed")
	}

	log.Info().Str("component", "balancer").
		Dur("gossip_interval", gossipInterval).
		Float64("trigger_pct", clusterCfg.BalancerImbalanceTriggerPct()).
		Float64("stop_pct", clusterCfg.BalancerImbalanceStopPct()).
		Msg("balancer started")
	return balancer, receiver, nil
}
