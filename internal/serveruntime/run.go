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

// Run is the cluster-mode server entry point. cmd/grainfs/runServe builds a
// Config from cobra flags and pre-resolves auth/encryptor inputs, then calls
// Run. Run owns lifecycle: starts every component, blocks on ctx.Done(), and
// orchestrates graceful shutdown.
//
// Boot decomposition (PRs 1-7, see docs/superpowers/specs/
// 2026-05-08-serveruntime-boot-decomposition.md): phase functions populate
// state and register cleanup. Cleanup runs LIFO at function exit.
func Run(ctx context.Context, cfg Config) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	state := newBootState(cfg)
	state.cancel = cancel
	defer state.Cleanup()

	// PR 2: config + storage open.
	if err := bootValidateConfig(state); err != nil {
		return err
	}
	if err := bootAutoMigrate(state); err != nil {
		return err
	}
	if err := bootOpenMetaDB(state); err != nil {
		return err
	}
	if err := bootValidateTimings(state); err != nil {
		return err
	}
	if err := bootOpenSharedFSMDB(state); err != nil {
		return err
	}

	// PR 3: transport.
	if err := bootQUICTransport(ctx, state); err != nil {
		return err
	}
	if err := bootPeerConnections(ctx, state); err != nil {
		return err
	}
	// groupRaftMux must exist BEFORE NewMetaTransportQUICMux so the meta-raft
	// transport auto-registers on construction.
	if err := bootGroupRaftMux(state); err != nil {
		return err
	}

	// Construct the data-plane raft node here (between transport and meta-raft
	// phases) — consumed by both PR 4 (RPC transport wiring) and PR 5 (storage
	// runtime). Bootstrap runs in non-join mode.
	//
	// In join mode, state.peers carries the join target transport address
	// (used by PerformMetaJoin / bootPeerConnections); it is NOT a node-ID
	// list and must NOT be fed to raft as cfg.Peers — doing so makes v2's
	// reconstructConfig seed the initial voter set with a transport address
	// as a voter ID, triggering a term storm that step-downs n1 mid-promote.
	// The joiner enters the cluster as a learner via PromoteToVoter from the
	// leader's perspective; its local raft starts as a single-voter {selfID}.
	// state.joinMode is set by bootValidateConfig (Task 3 populates it from
	// the .join-pending sentinel file).
	raftPeers := state.peers
	if state.joinMode {
		raftPeers = nil
	}
	raftCfg := raft.DefaultConfig(state.nodeID, raftPeers)
	raftCfg.ManagedMode = true
	raftCfg.LogGCInterval = cfg.RaftLogGCInterval
	// JoinMode is forwarded to v2 so the joiner's solo-voter local config
	// ({selfID} when raftPeers is nil) does NOT auto-promote to Leader —
	// see internal/raft/v2/types.go JoinMode docstring. Without this gate
	// the joiner becomes a phantom leader of its own 1-node cluster and
	// the cluster leader's joint AddVoter wait deadlocks.
	raftCfg.JoinMode = state.joinMode

	// M5 PR 29: raft v2 is the only path. The GRAINFS_RAFT_V2 flag is gone;
	// v1 (*raft.Node) is unreachable from serveruntime. PR 30 deletes the v1
	// package outright. Durable LogStore + StableStore + SnapshotStore live
	// in <raftDir>/raft-v2/.
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
	// M5 PR 27: wire the v2 QUIC RPC bridge so multi-node v2 clusters can
	// exchange Raft RPCs. The bridge re-implements v1's QUIC RPC dispatch on
	// top of cluster.RaftNode.Handle* (the v2 adapter translates to
	// raftv2.Node). Wire format is byte-identical to v1; v1 is frozen until
	// PR 30 deletes it.
	v2RPCTransport := cluster.NewRaftQUICRPCTransport(state.quicTransport, v2Node)
	v2RPCTransport.SetTransport()
	v2RPCTransport.SetTimeoutNowTransport()
	log.Info().Msg("raft v2: QUIC RPC transport wired (TimeoutNow enabled)")

	// PR 4: meta-raft callback registration BEFORE Start.
	if err := bootMetaRaftWiring(state); err != nil {
		return err
	}
	if err := bootDataGroupRouter(state); err != nil {
		return err
	}
	if err := bootRotationAndAdminAPI(state); err != nil {
		return err
	}
	if err := bootMetaRaftStart(ctx, state, StartRotationSocket); err != nil {
		return err
	}

	recordStartupDecision := func(decision badgerrole.Decision) {
		recordBadgerStartupDecision(state, decision)
	}

	// PR 5: storage runtime.
	if err := bootShardService(ctx, state); err != nil {
		return err
	}
	if err := bootStreamRouter(state); err != nil {
		return err
	}
	if err := bootOwnedGroupsAndEC(ctx, state, recordStartupDecision); err != nil {
		return err
	}

	// PR 6: snapshot + apply-loop.
	if err := bootSnapshotAndApplyLoop(state); err != nil {
		return fmt.Errorf("failed to initialize distributed storage: %w", err)
	}
	// §5 T44: reconcile the trusted-proxy.cidr atomic snapshot once after raft
	// start. Snapshot Restore (meta_fsm.go:3233) does NOT fire reload hooks,
	// so if the node booted from a restored snapshot the atomic-snapshot view
	// used by the iam.anon-enabled reload hook is still "" until this seeds it.
	// Done here (right after apply-loop start) rather than later so the
	// hook is correct from the first apply.
	if state.refreshProxyCIDR != nil && state.cfgStore != nil {
		v, _ := state.cfgStore.GetString("trusted-proxy.cidr")
		state.refreshProxyCIDR(v)
	}

	// PR-final: services + shutdown.
	if err := bootBalancerAndGossip(ctx, state); err != nil {
		return err
	}
	if err := bootWALAndForwarders(ctx, state); err != nil {
		return err
	}
	if err := bootBackendWrap(ctx, state); err != nil {
		return err
	}
	if err := bootSrvOptsAndReceipt(ctx, state); err != nil {
		return err
	}
	if err := bootHTTPServerAndAdmin(state); err != nil {
		return err
	}
	// §5 T44: refuse to start with anon-disabled + no TLS cert + no trusted
	// proxy. Must run AFTER bootHTTPServerAndAdmin (state.cfgStore + state.srv
	// populated) and BEFORE bootResharderAndDegraded (which goroutines
	// srv.Run() — the listener actually starts there).
	if err := bootTLSPostureGate(state); err != nil {
		return err
	}
	if err := bootRecoveryAndScrubber(ctx, state); err != nil {
		return err
	}
	if err := bootResharderAndDegraded(ctx, state); err != nil {
		return err
	}
	if err := bootNodeServices(ctx, state); err != nil {
		return err
	}

	// After a successful join-mode boot, remove the pending file and backups.
	// Backups (*.pre-join-backup) were safety nets for a failed wipe; now that
	// join succeeded they are no longer needed.
	if state.joinMode {
		pendingFile := filepath.Join(cfg.DataDir, JoinPendingFile)
		_ = os.Remove(pendingFile)
		for _, dir := range []string{"meta_raft", "raft", "shared-raft-log"} {
			_ = os.RemoveAll(filepath.Join(cfg.DataDir, dir+".pre-join-backup"))
		}
		log.Info().Str("peer", state.joinAddr).Msg("join complete — pending file removed")
	}

	bootShutdownDrain(ctx, state)
	return nil
}
