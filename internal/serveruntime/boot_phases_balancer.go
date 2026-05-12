package serveruntime

import (
	"context"

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
	if ccfg.BalancerEnabled() {
		statsStore := cluster.NewNodeStatsStore(3 * ccfg.BalancerGossipInterval())
		bp, gr, err := StartBalancer(ctx, state.nodeID, cfg.DataDir, statsStore, state.node, state.peers, state.fsm, state.quicTransport, state.shardSvc, state.effectiveEC.NumShards(), ccfg, ccfg)
		if err != nil {
			log.Warn().Err(err).Msg("balancer start failed")
		}
		state.balancerProposer = bp
		state.gossipReceiver = gr
	}

	// When balancer is off but heal-receipt is on, create a bare receiver;
	// its NodeStatsStore is unused in this path but required by the ctor.
	if state.gossipReceiver == nil && cfg.HealReceiptEnabled {
		standaloneStats := cluster.NewNodeStatsStore(3 * ccfg.BalancerGossipInterval())
		state.gossipReceiver = cluster.NewGossipReceiver(state.quicTransport, standaloneStats)
		go state.gossipReceiver.Run(ctx)
		log.Info().Str("component", "gossip").Msg("gossip receiver started (receipt-only, balancer disabled)")
	}
	return nil
}
