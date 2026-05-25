package serveruntime

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/badgerrole"
	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/raft"
)

type bootRuntimeDeps struct {
	startRotationSocket func(context.Context, string, *cluster.MetaRaft) error
}

type bootPhase struct {
	name string
	run  func(context.Context, *bootState) error
}

func runBootRuntime(ctx context.Context, state *bootState, deps bootRuntimeDeps) error {
	return runBootPhases(ctx, state, productionBootPhases(deps))
}

func runBootPhases(ctx context.Context, state *bootState, phases []bootPhase) error {
	for _, phase := range phases {
		if err := phase.run(ctx, state); err != nil {
			return err
		}
	}
	return nil
}

func productionBootPhases(deps bootRuntimeDeps) []bootPhase {
	return []bootPhase{
		{name: "validate-config", run: func(_ context.Context, state *bootState) error {
			return bootValidateConfig(state)
		}},
		{name: "auto-migrate", run: func(_ context.Context, state *bootState) error {
			return bootAutoMigrate(state)
		}},
		{name: "open-meta-db", run: func(_ context.Context, state *bootState) error {
			return bootOpenMetaDB(state)
		}},
		{name: "validate-timings", run: func(_ context.Context, state *bootState) error {
			return bootValidateTimings(state)
		}},
		{name: "open-shared-fsm-db", run: func(_ context.Context, state *bootState) error {
			return bootOpenSharedFSMDB(state)
		}},
		{name: "quic-transport", run: bootQUICTransport},
		{name: "peer-connections", run: bootPeerConnections},
		{name: "group-raft-mux", run: func(_ context.Context, state *bootState) error {
			return bootGroupRaftMux(state)
		}},
		{name: "data-raft-node", run: func(_ context.Context, state *bootState) error {
			return bootDataRaftNode(state)
		}},
		{name: "meta-raft-wiring", run: func(_ context.Context, state *bootState) error {
			return bootMetaRaftWiring(state)
		}},
		{name: "data-group-router", run: func(_ context.Context, state *bootState) error {
			return bootDataGroupRouter(state)
		}},
		{name: "rotation-admin-api", run: func(_ context.Context, state *bootState) error {
			return bootRotationAndAdminAPI(state)
		}},
		{name: "meta-raft-start", run: func(ctx context.Context, state *bootState) error {
			return bootMetaRaftStart(ctx, state, deps.startRotationSocket)
		}},
		{name: "shard-service", run: bootShardService},
		{name: "stream-router", run: func(_ context.Context, state *bootState) error {
			return bootStreamRouter(state)
		}},
		{name: "owned-groups-ec", run: func(ctx context.Context, state *bootState) error {
			return bootOwnedGroupsAndEC(ctx, state, func(decision badgerrole.Decision) {
				recordBadgerStartupDecision(state, decision)
			})
		}},
		{name: "snapshot-apply-loop", run: func(_ context.Context, state *bootState) error {
			if err := bootSnapshotAndApplyLoop(state); err != nil {
				return fmt.Errorf("failed to initialize distributed storage: %w", err)
			}
			return nil
		}},
		{name: "post-snapshot-config-seed", run: func(_ context.Context, state *bootState) error {
			seedPostSnapshotConfig(state)
			return nil
		}},
		{name: "balancer-gossip", run: bootBalancerAndGossip},
		{name: "wal-forwarders", run: bootWALAndForwarders},
		{name: "backend-wrap", run: bootBackendWrap},
		{name: "srvopts-receipt", run: bootSrvOptsAndReceipt},
		{name: "http-server-admin", run: func(_ context.Context, state *bootState) error {
			return bootHTTPServerAndAdmin(state)
		}},
		{name: "tls-posture-gate", run: func(_ context.Context, state *bootState) error {
			return bootTLSPostureGate(state)
		}},
		{name: "phase0-banner", run: func(_ context.Context, state *bootState) error {
			return bootPhase0Banner(state)
		}},
		{name: "recovery-scrubber", run: bootRecoveryAndScrubber},
		{name: "resharder-degraded", run: bootResharderAndDegraded},
		{name: "node-services", run: bootNodeServices},
		{name: "join-complete-cleanup", run: func(_ context.Context, state *bootState) error {
			cleanupJoinCompletion(state)
			return nil
		}},
	}
}

func bootDataRaftNode(state *bootState) error {
	// In join mode, state.peers is the join target transport address, not a
	// voter ID list. The data-plane raft node must start as solo {selfID} and
	// wait for the leader-side PromoteToVoter path.
	raftPeers := state.peers
	if state.joinMode {
		raftPeers = nil
	}
	raftCfg := raft.DefaultConfig(state.nodeID, raftPeers)
	raftCfg.ManagedMode = true
	raftCfg.LogGCInterval = state.cfg.RaftLogGCInterval
	raftCfg.JoinMode = state.joinMode

	v2Node, v2Close, err := cluster.NewRaftV2NodeForServeruntime(raftCfg, state.raftDir)
	if err != nil {
		return fmt.Errorf("raft v2 init: %w", err)
	}
	state.AddCleanup(func() {
		if v2Close != nil {
			_ = v2Close()
		}
	})
	if !state.joinMode {
		if err := v2Node.Bootstrap(); err != nil && !errors.Is(err, raft.ErrAlreadyBootstrapped) {
			return fmt.Errorf("raft v2 bootstrap: %w", err)
		}
	}
	state.node = v2Node
	// Wire the v2 QUIC RPC bridge before meta-raft/storage phases consume the
	// data-plane raft node.
	state.rpcTransport = cluster.NewRaftQUICRPCTransport(state.quicTransport, v2Node)
	state.rpcTransport.SetTransport()
	state.rpcTransport.SetTimeoutNowTransport()
	log.Info().Msg("raft v2: QUIC RPC transport wired (TimeoutNow enabled)")
	return nil
}

func seedPostSnapshotConfig(state *bootState) {
	if state.refreshProxyCIDR == nil || state.cfgStore == nil {
		return
	}
	v, _ := state.cfgStore.GetString("trusted-proxy.cidr")
	state.refreshProxyCIDR(v)
	if state.proxyTrust != nil {
		state.proxyTrust.SetCIDRs(splitTrustedProxyCIDRSpec(v))
	}
}

func cleanupJoinCompletion(state *bootState) {
	if !state.joinMode {
		return
	}
	pendingFile := filepath.Join(state.cfg.DataDir, JoinPendingFile)
	_ = os.Remove(pendingFile)
	for _, dir := range []string{"meta_raft", "raft", "shared-raft-log"} {
		_ = os.RemoveAll(filepath.Join(state.cfg.DataDir, dir+".pre-join-backup"))
	}
	log.Info().Str("peer", state.joinAddr).Msg("join complete — pending file removed")
}
