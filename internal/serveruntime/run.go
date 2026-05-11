package serveruntime

import (
	"context"
	"errors"
	"fmt"

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
	state := newBootState(cfg)
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
	if err := bootOpenRaftLogStore(state); err != nil {
		return err
	}
	if err := bootOpenSharedRaftLogDB(state); err != nil {
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
	raftCfg := raft.DefaultConfig(state.nodeID, state.peers)
	raftCfg.ManagedMode = cfg.BadgerManagedMode
	raftCfg.LogGCInterval = cfg.RaftLogGCInterval

	if cluster.IsV2Enabled("serveruntime") {
		// M5 PR 26: GRAINFS_RAFT_V2=serveruntime — construct the v2 node
		// behind the cluster.RaftNode interface. Durable LogStore +
		// StableStore + SnapshotStore live in <raftDir>/raft-v2/ (sibling
		// to the v1 raft/ directory; v1 and v2 on-disk schemas differ).
		v2Node, v2Close, err := cluster.NewRaftV2NodeForServeruntime(raftCfg, state.raftDir)
		if err != nil {
			return fmt.Errorf("raft v2 init: %w", err)
		}
		state.AddCleanup(func() {
			if v2Close != nil {
				_ = v2Close()
			}
		})
		if !cfg.JoinMode {
			if err := v2Node.Bootstrap(); err != nil && !errors.Is(err, raft.ErrAlreadyBootstrapped) {
				return fmt.Errorf("raft v2 bootstrap: %w", err)
			}
		}
		state.node = v2Node
		// M5 PR 27: wire the v2 QUIC RPC bridge so multi-node v2 clusters
		// can exchange Raft RPCs. The bridge re-implements v1's QUIC RPC
		// dispatch on top of cluster.RaftNode.Handle* (the v2 adapter
		// translates to raftv2.Node). Wire format is byte-identical to v1
		// (see internal/cluster/raftv2_quic_codec.go); v1 is frozen until
		// PR 30 deletes it.
		v2RPCTransport := cluster.NewRaftV2QUICRPCTransport(state.quicTransport, v2Node)
		v2RPCTransport.SetTransport()
		log.Info().Msg("raft v2: QUIC RPC transport wired (M5 PR 27)")
	} else {
		node := raft.NewNode(raftCfg, state.logStore)
		if !cfg.JoinMode {
			if err := node.Bootstrap(); err != nil && !errors.Is(err, raft.ErrAlreadyBootstrapped) {
				return fmt.Errorf("raft bootstrap: %w", err)
			}
		}
		state.node = node
		rpcTransport := raft.NewQUICRPCTransport(state.quicTransport, node)
		rpcTransport.SetTransport()
		state.rpcTransport = rpcTransport
	}

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
		state.startupDecisions = append(state.startupDecisions, decision)
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
	if err := bootRecoveryAndScrubber(ctx, state); err != nil {
		return err
	}
	if err := bootResharderAndDegraded(ctx, state); err != nil {
		return err
	}
	if err := bootNodeServices(ctx, state); err != nil {
		return err
	}

	bootShutdownDrain(ctx, state)
	return nil
}
