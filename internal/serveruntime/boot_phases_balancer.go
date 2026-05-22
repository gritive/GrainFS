package serveruntime

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster"
)

// bootBalancerAndGossip starts the balancer (cluster mode only) and ensures a
// single GossipReceiver drains transport.Receive() whenever a feature needs
// StreamReceipt gossip. Only one consumer is allowed because Receive() is a
// single channel — competing readers would deliver each message to only one.
//
// Inputs:  state.cfg (HealReceiptEnabled), state.metaRaft.FSM().ClusterConfig()
//
//	(balancer enabled flag + gossip interval), state.nodeID, state.peers,
//	state.cfg.DataDir, state.node, state.fsm, state.quicTransport,
//	state.shardSvc, state.effectiveEC.
//
// Outputs: state.balancerProposer, state.gossipReceiver.
//
// Phase ordering rationale: must run AFTER bootSnapshotAndApplyLoop (state.fsm
// populated) and BEFORE any heal-receipt wiring (SetupClusterReceipt expects a
// gossipReceiver to bind the receipt cache onto).
func bootBalancerAndGossip(ctx context.Context, state *bootState) error {
	cfg := state.cfg
	ccfg := state.metaRaft.FSM().ClusterConfig()
	gossipPeerProvider := func() []string {
		return receiptPeerAddresses(state.nodeID, state.raftAddr, state.peers, state.metaRaft.FSM().Nodes())
	}
	if ccfg.BalancerEnabled() {
		statsStore := cluster.NewNodeStatsStore(3 * ccfg.BalancerGossipInterval())
		bp, gr, err := StartBalancer(ctx, state.nodeID, cfg.DataDir, statsStore, state.node, state.peers, state.fsm, state.quicTransport, state.shardSvc, state.effectiveEC.NumShards(), ccfg, ccfg, state.capabilityGate, state.metaRaft.FSM(), state.metaRaft.FSM(), gossipPeerProvider)
		if err != nil {
			log.Warn().Err(err).Msg("balancer start failed")
		}
		state.balancerProposer = bp
		state.gossipReceiver = gr
		// Wire gossip-fed stats + live ClusterConfig into placement. Weighted
		// placement and BoundedLoads skip are inactive until this is called.
		// When balancer is disabled the backend retains nodeStatsStore==nil
		// and falls back to legacy unweighted placement.
		state.distBackend.StartPlacementRuntime(ctx, ccfg, statsStore)
	}

	needsCapabilityGossip := state.capabilityGate != nil

	// When balancer is off but heal-receipt or capability evidence needs
	// gossip, create a bare receiver; its NodeStatsStore is unused by receipt
	// but required by the ctor.
	if state.gossipReceiver == nil && (cfg.HealReceiptEnabled || needsCapabilityGossip) {
		standaloneStats := cluster.NewNodeStatsStore(3 * ccfg.BalancerGossipInterval())
		state.gossipReceiver = cluster.NewGossipReceiver(state.quicTransport, standaloneStats).
			WithCapabilityGate(state.capabilityGate).
			WithNodeAddressBook(state.metaRaft.FSM())
		go state.gossipReceiver.Run(ctx)
		log.Info().Str("component", "gossip").Msg("gossip receiver started")
	}
	if state.gossipReceiver != nil {
		state.gossipReceiver.SetCapabilityGate(state.capabilityGate)
		state.gossipReceiver.SetNodeAddressBook(state.metaRaft.FSM())
	}
	if needsCapabilityGossip && !ccfg.BalancerEnabled() {
		statsStore := cluster.NewNodeStatsStore(3 * ccfg.BalancerGossipInterval())
		statsStore.Set(cluster.NodeStats{NodeID: state.nodeID, JoinedAt: time.Now()})
		sender := cluster.NewGossipSender(state.nodeID, state.peers, state.quicTransport, statsStore, ccfg.BalancerGossipInterval()).
			WithPeerProvider(gossipPeerProvider).
			WithCapabilityEvidenceSource(state.metaRaft.FSM()).
			WithCapabilityGate(state.capabilityGate).
			WithCapabilityEvidenceAliases(state.raftAddr)
		go sender.Run(ctx)
		log.Info().Str("component", "gossip").Msg("capability evidence gossip sender started")
	}
	return nil
}
