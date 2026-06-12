package serveruntime

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/gossip"
)

// bootBalancerAndGossip starts the balancer (cluster mode only) and ensures a
// single GossipReceiver consumes the native gossip routes whenever a feature
// needs receipt gossip.
//
// Inputs:  state.cfg (HealReceiptEnabled), state.metaRaft.FSM().ClusterConfig()
//
//	(balancer enabled flag + gossip interval), state.nodeID, state.peers,
//	state.cfg.DataDir, state.node, state.fsm, state.clusterTransport,
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
		statsStore := gossip.NewNodeStatsStore(3 * ccfg.BalancerGossipInterval())
		bp, gr, err := StartBalancer(ctx, state.nodeID, cfg.DataDir, statsStore, state.node, state.peers, state.fsm, state.clusterTransport, state.shardSvc, state.effectiveEC.NumShards(), ccfg, ccfg, state.capabilityGate, state.metaRaft.FSM(), state.metaRaft.FSM(), gossipPeerProvider)
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
		state.placementStatsStore = statsStore
	}

	needsCapabilityGossip := state.capabilityGate != nil

	// When balancer is off but heal-receipt or capability evidence needs
	// gossip, create a bare receiver; its NodeStatsStore is unused by receipt
	// but required by the ctor.
	if state.gossipReceiver == nil && (cfg.HealReceiptEnabled || needsCapabilityGossip) {
		standaloneStats := gossip.NewNodeStatsStore(3 * ccfg.BalancerGossipInterval())
		state.gossipReceiver = gossip.NewGossipReceiver(state.clusterTransport, standaloneStats).
			WithCapabilityGate(state.capabilityGate).
			WithAddressResolver(cluster.NodeAddressBookResolver(state.metaRaft.FSM()))
		// Phase 8 N7-3: native gossip routes replace the Receive()-loop goroutine.
		state.gossipReceiver.RegisterNativeGossipRoutes()
		log.Info().Str("component", "gossip").Msg("gossip receiver started")
	}
	if state.gossipReceiver != nil {
		state.gossipReceiver.SetCapabilityGate(state.capabilityGate)
		state.gossipReceiver.SetAddressResolver(cluster.NodeAddressBookResolver(state.metaRaft.FSM()))
	}
	if needsCapabilityGossip && !ccfg.BalancerEnabled() {
		statsStore := gossip.NewNodeStatsStore(3 * ccfg.BalancerGossipInterval())
		statsStore.Set(gossip.NodeStats{NodeID: state.nodeID, JoinedAt: time.Now()})
		sender := gossip.NewGossipSender(state.nodeID, state.peers, state.clusterTransport, statsStore, ccfg.BalancerGossipInterval()).
			WithPeerProvider(gossipPeerProvider).
			WithCapabilityEvidenceSource(state.metaRaft.FSM()).
			WithCapabilityGate(state.capabilityGate).
			WithCapabilityEvidenceAliases(state.raftAddr)
		go sender.Run(ctx)
		log.Info().Str("component", "gossip").Msg("capability evidence gossip sender started")
	}
	return nil
}
