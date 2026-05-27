package serveruntime

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

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
	// §5 T46: default banner sink. Tests using bootstrap.Run override
	// state.bannerWriter to a buffer before phase dispatch.
	state.bannerWriter = os.Stdout
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
	if state.joinMode || state.inviteJoinMode {
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
	raftCfg.JoinMode = state.joinMode || state.inviteJoinMode

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
	if !state.joinMode && !state.inviteJoinMode {
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
	// Task 11: KEK rotation leader + peer probe handlers + audit sink. MUST
	// run before bootMetaRaftStart so SetKEKRotationLeader lands before the
	// leadership watcher (started by Start) reads it.
	if err := bootKEKRotationLeader(state); err != nil {
		return err
	}
	// §7 T57: bootMetaRaftStart's preApplyLoop callback handles post-Restore
	// DEK-keeper reconstruction (F#21 / F#22) atomically between Restore and
	// the apply-loop launch — see rebuildDEKKeeperFromRestore.
	if err := bootMetaRaftStart(ctx, state, StartRotationSocket); err != nil {
		return err
	}
	// Phase D Task 5: on a fresh genesis boot (single voter), replicate the
	// locally-generated DEK gen-0 through the ungated bootstrap propose so
	// joiners install identical bytes. No-op on joiners / restarts (not genesis).
	if err := bootGenesisDEKBootstrap(ctx, state); err != nil {
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
		// §5 T45: same snapshot-Restore-doesn't-fire-hooks problem — seed the
		// ProxyTrust CIDR set from the restored cfgStore so authoritativeClientIP
		// is correct from the first request post-Restore.
		if state.proxyTrust != nil {
			state.proxyTrust.SetCIDRs(splitTrustedProxyCIDRSpec(v))
		}
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
	// §5 T46: print Phase 0 anonymous-access banner once at startup. Placed
	// AFTER bootTLSPostureGate so a refused boot does not contradict itself
	// by also printing the warning.
	if err := bootPhase0Banner(state); err != nil {
		return err
	}
	if err := bootRecoveryAndScrubber(ctx, state); err != nil {
		return err
	}

	// Phase D Task 6: do not accept user-facing encrypted-data traffic until
	// the active DEK is installed (gen-0 via genesis Apply, or via join replay /
	// snapshot restore on a joiner). Bounded so a joiner that cannot install
	// never deadlocks boot — it fails fast instead. Genesis single-node is
	// ready immediately.
	if state.dekKeeper != nil {
		readyCtx, readyCancel := context.WithTimeout(ctx, dekReadyBootTimeout)
		err := WaitDEKReady(readyCtx, state.dekKeeper)
		readyCancel()
		if err != nil {
			return fmt.Errorf("DEK readiness: %w", err)
		}
	}

	// Phase D Task 7: refuse to boot if the replayed raft log contained a
	// legacy type-48 DEKRotate (pre-Phase-D). Live-log path guard; snapshot
	// path is covered by LoadFromFSM AAD-unwrap failing during Restore.
	//
	// REPLAY-ORDERING BARRIER: applyDEKRotate sets legacyDEKRotateSeen during
	// the apply loop, which runs ASYNC after MetaRaft.Start() returns. We must
	// drain the apply loop up to the current COMMITTED index before reading the
	// flag, or the guard passes silently on an un-replayed legacy log.
	//
	// We target CommittedIndex (captured HERE, not at Start). This phase runs
	// late in boot — after bootMetaRaftStart, the becomeLeader no-op (single
	// voter), join, and AppendEntries catch-up (follower) have all advanced
	// commit past the snapshot floor to cover every legacy type-48 entry that
	// was committed on the pre-Phase-D cluster. Committed entries are exactly
	// the ones that WILL apply, so waiting on them is sufficient. We do NOT wait
	// on LastLogIndex: an uncommitted tail (e.g. a former leader that appended
	// but never replicated before crashing) may be truncated by the new leader
	// and would otherwise block this drain until the 10s timeout, failing boot
	// spuriously during leader churn. Bounded: a genuinely stuck apply fails
	// loud rather than booting divergent.
	if state.metaRaft != nil {
		if committed := state.metaRaft.Node().CommittedIndex(); committed > 0 {
			drainCtx, drainCancel := context.WithTimeout(ctx, 10*time.Second)
			err := state.metaRaft.WaitApplied(drainCtx, committed)
			drainCancel()
			if err != nil {
				return fmt.Errorf("greenfield DEK boundary: wait for log drain: %w", err)
			}
		}
		if err := state.metaRaft.FSM().CheckGreenfieldDEKBoundary(); err != nil {
			return fmt.Errorf("greenfield DEK boundary: %w", err)
		}
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

	// Zero-CA invite-join (W9b): the Phase-2 ACK in bootWALAndForwarders already
	// cleared the .invite-join-pending sentinel + shredded node.key.unsealed on
	// success. Nothing left to clean here — the sentinel removal is the resume
	// barrier, owned by the Phase-2 path.

	bootShutdownDrain(ctx, state)
	return nil
}
