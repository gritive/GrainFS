package serveruntime

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/transport"
)

// BalancerOptions captures the cobra-flag-derived knobs that StartBalancer
// previously read directly from a cobra.Command. Caller (cmd) is
// responsible for translating flag values into this struct.
type BalancerOptions struct {
	GossipInterval      time.Duration
	WarmupTimeout       time.Duration
	ImbalanceTriggerPct float64
	ImbalanceStopPct    float64
	MigrationRate       int
	LeaderTenureMin     time.Duration
	CBThreshold         float64
	MigrationMaxRetries int
	MigrationPendingTTL time.Duration
}

// RaftBalancerAdapter wraps *raft.Node to implement
// cluster.RaftBalancerNode. Used internally by StartBalancer; exported so
// tests outside this package can supply their own adapter when wiring a
// fake balancer.
type RaftBalancerAdapter struct {
	node  *raft.Node
	peers []string
}

func NewRaftBalancerAdapter(node *raft.Node, peers []string) *RaftBalancerAdapter {
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
func StartBalancer(
	ctx context.Context,
	opts BalancerOptions,
	nodeID, dataDir string,
	statsStore *cluster.NodeStatsStore,
	node *raft.Node,
	peers []string,
	fsm *cluster.FSM,
	quicTransport transport.Transport,
	shardSvc *cluster.ShardService,
	numShards int,
) (*cluster.BalancerProposer, *cluster.GossipReceiver, error) {
	if opts.CBThreshold < 0 || opts.CBThreshold > 1 {
		return nil, nil, fmt.Errorf("balancer cb-threshold must be in [0, 1], got %g", opts.CBThreshold)
	}

	def := cluster.DefaultBalancerConfig()
	cfg := cluster.BalancerConfig{
		GossipInterval:      opts.GossipInterval,
		WarmupTimeout:       opts.WarmupTimeout,
		ImbalanceTriggerPct: opts.ImbalanceTriggerPct,
		ImbalanceStopPct:    opts.ImbalanceStopPct,
		MigrationRate:       opts.MigrationRate,
		LeaderTenureMin:     opts.LeaderTenureMin,
		LeaderLoadThreshold: def.LeaderLoadThreshold,
		GracePeriod:         def.GracePeriod,
		PeerSeenWindow:      def.PeerSeenWindow,
		CBThreshold:         opts.CBThreshold,
		MigrationMaxRetries: opts.MigrationMaxRetries,
		MigrationPendingTTL: opts.MigrationPendingTTL,
	}

	adapter := NewRaftBalancerAdapter(node, peers)
	balancer := cluster.NewBalancerProposer(nodeID, statsStore, adapter, cfg)

	balancer.SetObjectPicker(cluster.NewLocalObjectPicker(filepath.Join(dataDir, "shards")))

	taskCh := make(chan cluster.MigrationTask, 256)

	exec := cluster.NewMigrationExecutorWithTTL(shardSvc, adapter, numShards, opts.MigrationPendingTTL)
	if opts.MigrationMaxRetries > 0 {
		exec.SetMaxWriteRetries(opts.MigrationMaxRetries)
	}
	exec.SetShardCounter(ECShardCounterFor(fsm))
	exec.Start(ctx)

	fsm.SetMigrationHooks(taskCh, exec, balancer)

	sender := cluster.NewGossipSender(nodeID, peers, quicTransport, statsStore, opts.GossipInterval)
	receiver := cluster.NewGossipReceiver(quicTransport, statsStore)

	go sender.Run(ctx)
	go receiver.Run(ctx)
	go exec.Run(ctx, taskCh)
	go balancer.Run(ctx)

	statsStore.Set(cluster.NodeStats{
		NodeID:   nodeID,
		JoinedAt: time.Now(),
	})

	collector := cluster.NewDiskCollector(nodeID, dataDir, statsStore, opts.GossipInterval)
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

	if err := fsm.RecoverPending(ctx, taskCh); err != nil {
		log.Warn().Err(err).Msg("balancer: recover pending failed")
	}

	log.Info().Str("component", "balancer").
		Dur("gossip_interval", opts.GossipInterval).Float64("trigger_pct", opts.ImbalanceTriggerPct).Float64("stop_pct", opts.ImbalanceStopPct).Msg("balancer started")
	return balancer, receiver, nil
}
