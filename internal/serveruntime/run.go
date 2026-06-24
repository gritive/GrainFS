package serveruntime

import (
	"context"
	"errors"
	"fmt"
	"os"
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
	if err := bootRaftStoreKey(state); err != nil {
		return fmt.Errorf("raft store key: %w", err)
	}

	// PR 3: transport.
	if err := bootClusterTransport(ctx, state); err != nil {
		return err
	}
	// groupRaftMux must exist BEFORE NewMetaTransport so the meta-raft
	// transport auto-registers on construction.
	if err := bootGroupRaftMux(state); err != nil {
		return err
	}

	// Construct the data-plane raft node here (between transport and meta-raft
	// phases) — consumed by both PR 4 (RPC transport wiring) and PR 5 (storage
	// runtime). Bootstrap runs in non-join mode.
	//
	// In join mode, state.peers carries the join target transport address
	// (used by PerformMetaJoin); it is NOT a node-ID
	// list and must NOT be fed to raft as cfg.Peers — doing so makes v2's
	// reconstructConfig seed the initial voter set with a transport address
	// as a voter ID, triggering a term storm that step-downs n1 mid-promote.
	// The joiner enters the cluster as a learner via PromoteToVoter from the
	// leader's perspective; its local raft starts as a single-voter {selfID}.
	// state.joinMode is set by bootValidateConfig (Task 3 populates it from
	// the .join-pending sentinel file).
	raftPeers := state.peers
	if state.inviteJoinMode {
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
	raftCfg.JoinMode = state.inviteJoinMode

	// M5 PR 29: raft v2 is the only path. The GRAINFS_RAFT_V2 flag is gone;
	// v1 (*raft.Node) is unreachable from serveruntime. PR 30 deletes the v1
	// package outright. Durable LogStore + StableStore + SnapshotStore live
	// in <raftDir>/raft-v2/.
	v2Node, v2Close, err := cluster.NewRaftV2NodeForServeruntimeWithStoreOptions(raftCfg, state.raftDir, cluster.RaftV2StoreOptions{
		EncryptionKey: state.raftStoreKey,
	})
	if err != nil {
		return fmt.Errorf("raft v2 init: %w", err)
	}
	state.AddCleanup(func() {
		if v2Close != nil {
			_ = v2Close()
		}
	})
	if !state.inviteJoinMode {
		if err := v2Node.Bootstrap(); err != nil && !errors.Is(err, raft.ErrAlreadyBootstrapped) {
			return fmt.Errorf("raft v2 bootstrap: %w", err)
		}
	}
	state.node = v2Node
	// M5 PR 27: wire the v2 Raft RPC bridge so multi-node v2 clusters can
	// exchange Raft RPCs. The bridge re-implements v1's Raft RPC dispatch on
	// top of cluster.RaftNode.Handle* (the v2 adapter translates to
	// raftv2.Node). Wire format is byte-identical to v1; v1 is frozen until
	// PR 30 deletes it.
	v2RPCTransport := cluster.NewRaftRPCTransport(state.clusterTransport, v2Node)
	v2RPCTransport.SetTransport()
	v2RPCTransport.SetTimeoutNowTransport()
	log.Info().Msg("raft v2: Raft RPC transport wired (TimeoutNow enabled)")
	// Start the data-raft actor IMMEDIATELY after the RPC bridge is wired —
	// BEFORE invite-join Phase-2 (inside bootWALAndForwardersPart1). During
	// Phase-2 the leader's post-join hook calls AddVoterCtx and replicates
	// AppendEntries to this joiner; if the actor isn't draining cmdCh yet,
	// HandleAppendEntries blocks forever and the leader's AddVoter times out —
	// the join deadlocks (the long-standing 5-node EC e2e failure, TODOS §6).
	// Early start is safe: the applyLoop buffers applied entries UNBOUNDED
	// until distBackend.RunApplyLoop (bootOwnedGroupsAndEC) drains them, and
	// JoinMode (set above for invite-joiners) suppresses both the solo-voter
	// auto-promote and election-timer campaigns until the leader's AE installs
	// the real multi-voter config (actor.go JoinMode guards).
	state.node.Start()
	state.AddCleanup(func() { state.node.Close() })

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

	// R-FSM-α: forwarder/coordinator CONSTRUCTION + keeper-population catch-up
	// must run BEFORE WaitDEKReady so the meta-raft apply loop can install gen-0
	// into the keeper. Handler registration is deferred to
	// bootRegisterForwardHandlers (below, after bootShardService).
	if err := bootWALAndForwardersPart1(ctx, state); err != nil {
		return err
	}
	// R1 (narrow): gate here — the keeper is now guaranteed populated by the
	// join / invite catch-up at the end of bootWALAndForwardersPart1 (the
	// meta-raft apply loop, started in bootMetaRaftStart, installs gen-0).
	// Bounded so a joiner that cannot install fails fast instead of deadlocking.
	if state.dekKeeper != nil {
		readyCtx, readyCancel := context.WithTimeout(ctx, dekReadyBootTimeout)
		err := WaitDEKReady(readyCtx, state.dekKeeper)
		readyCancel()
		if err != nil {
			return fmt.Errorf("DEK readiness: %w", err)
		}
	}

	// R-FSM-α: PR 5 storage runtime now runs AFTER WaitDEKReady so the data
	// encryptor it constructs sees a populated DEK keeper.
	if err := bootShardService(ctx, state); err != nil {
		return err
	}
	if err := bootShardRoutes(state); err != nil {
		return err
	}
	if err := bootOwnedGroupsAndEC(ctx, state, recordStartupDecision); err != nil {
		return err
	}
	if err := bootClusterCoordinatorRouting(state); err != nil {
		return err
	}

	// PR 6: snapshot + apply-loop.
	if err := bootSnapshotAndApplyLoop(state); err != nil {
		return fmt.Errorf("failed to initialize distributed storage: %w", err)
	}

	if err := bootBackendWrap(ctx, state); err != nil {
		return err
	}
	// S6b: register the data-rewrap lanes now that distBackend (bootOwnedGroupsAndEC)
	// and packedBackend (bootBackendWrap) exist. wireDEKKeeper ran too early (before
	// the backends) to register them; it stored the controller on state for this.
	wireRewrapLanes(state)

	// R-FSM-α: register data-shard RPC handlers now that shardSvc exists. Until
	// this point, the forwarder routes answer 503 not-ready, so any racing
	// forwarded RPC sees a clean not-ready error rather than a panic.
	if err := bootRegisterForwardHandlers(state); err != nil {
		return err
	}

	// PR-final: services + shutdown.
	if err := bootBalancerAndGossip(ctx, state); err != nil {
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
	// populated) and BEFORE bootDegradedAndServices (which goroutines
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

	if err := bootDegradedAndServices(ctx, state); err != nil {
		return err
	}

	// Zero-CA §6 D-rev3 step 2: announce this node's own per-node SPKI into the
	// peer registry. Placed LAST in the post-join sequence — after
	// bootWALAndForwardersPart1 (forwarder installed) and after invite-join Phase-2
	// membership promotion (also inside bootWALAndForwardersPart1) — so this node is a
	// functioning meta-raft member whose Propose reaches the leader. PSK-bridged:
	// the booting node is still accepted via the PSK SPKI.
	if err := bootSelfRegisterMember(ctx, state); err != nil {
		return err
	}

	// Zero-CA invite-join (W9b): the Phase-2 ACK in bootWALAndForwardersPart1 already
	// cleared the .invite-join-pending sentinel + shredded node.key.unsealed on
	// success. Nothing left to clean here — the sentinel removal is the resume
	// barrier, owned by the Phase-2 path.

	bootShutdownDrain(ctx, state)
	return nil
}
