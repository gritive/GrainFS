package serveruntime

import (
	"context"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/compat"
	"github.com/gritive/GrainFS/internal/config"
	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/protocred"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/transport"
)

// bootDataRaftNode constructs the data-plane raft node (between the transport
// and meta-raft phases), wires the v2 Raft RPC bridge, and Starts the data-raft
// actor early. It is consumed by both PR 4 (RPC transport wiring) and PR 5
// (storage runtime). Bootstrap runs in non-join mode.
//
// MOVE-ONLY EXTRACTION (Slice 1): this body is the former Run() inline block
// verbatim; the only edit is cfg.RaftLogGCInterval → state.cfg.RaftLogGCInterval
// (the cfg param is gone now that this is a phase). ctx is reserved for the
// uniform phase signature and intentionally unused here. The escaping outputs
// are state.node = v2Node plus two state.AddCleanup registrations; every other
// local (v2Node/v2Close/v2RPCTransport/raftCfg/raftPeers) stays internal.
func bootDataRaftNode(ctx context.Context, state *bootState) error {
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
	raftCfg.LogGCInterval = state.cfg.RaftLogGCInterval
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
	return nil
}

// bootWaitDEKReady gates boot on the DEK keeper being populated. The keeper is
// guaranteed populated by the join / invite catch-up at the end of
// bootWALAndForwardersPart1 (the meta-raft apply loop, started in
// bootMetaRaftStart, installs gen-0). Bounded so a joiner that cannot install
// fails fast instead of deadlocking. No-op when encryption is disabled (nil
// keeper). Extracted from Run() in Slice 1 — behavior-preserving.
func bootWaitDEKReady(ctx context.Context, state *bootState) error {
	if state.dekKeeper != nil {
		readyCtx, readyCancel := context.WithTimeout(ctx, dekReadyBootTimeout)
		err := WaitDEKReady(readyCtx, state.dekKeeper)
		readyCancel()
		if err != nil {
			return fmt.Errorf("DEK readiness: %w", err)
		}
	}
	return nil
}

// bootGreenfieldDEKBoundary refuses to boot if the replayed raft log contained a
// legacy type-48 DEKRotate (pre-Phase-D). Live-log path guard; snapshot path is
// covered by LoadFromFSM AAD-unwrap failing during Restore. Extracted from Run()
// in Slice 1 — behavior-preserving.
//
// REPLAY-ORDERING BARRIER: applyDEKRotate sets legacyDEKRotateSeen during the
// apply loop, which runs ASYNC after MetaRaft.Start() returns. We must drain the
// apply loop up to the current COMMITTED index before reading the flag, or the
// guard passes silently on an un-replayed legacy log.
//
// We target CommittedIndex (captured HERE). This phase runs late in boot — after
// bootMetaRaftStart, the becomeLeader no-op (single voter), join, and
// AppendEntries catch-up (follower) have all advanced commit past the snapshot
// floor to cover every legacy type-48 entry that was committed on the
// pre-Phase-D cluster. Committed entries are exactly the ones that WILL apply, so
// waiting on them is sufficient. We do NOT wait on LastLogIndex: an uncommitted
// tail (e.g. a former leader that appended but never replicated before crashing)
// may be truncated by the new leader and would otherwise block this drain until
// the 10s timeout, failing boot spuriously during leader churn. Bounded: a
// genuinely stuck apply fails loud rather than booting divergent.
func bootGreenfieldDEKBoundary(ctx context.Context, state *bootState) error {
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
	return nil
}

// bootMetaRaftWiring constructs the meta-raft control plane and its cluster-transport
// transport. NO callbacks are registered and Start is NOT called here — that
// split is the central invariant of PRs 3-4: callbacks register on the FSM
// (bootDataGroupRouter, bootRotationAndAdminAPI) and Start runs only after
// every callback is in place (bootMetaRaftStart).
//
// IAM applier wiring also happens here (it does not register a runtime
// callback, just plumbs the apply path into the FSM).
func bootMetaRaftWiring(state *bootState) error {
	// In join mode, state.peers is the join target transport address (used by
	// PerformMetaJoin), not a meta-raft node-ID list. Passing it through as
	// MetaRaftConfig.Peers would seed the initial voter set with a transport
	// address as a voter ID — see run.go's raftPeers comment for the term-
	// storm mechanism. The joiner is added to meta-raft as a voter by the
	// leader after PerformMetaJoin succeeds.
	// state.joinMode is populated by bootValidateConfig (Task 3 sets it from
	// the .join-pending sentinel file).
	// inviteJoinMode is a join variant (zero-CA): like joinMode, the joiner must
	// NOT bootstrap its own meta-raft — the leader adds it as a voter via the
	// Phase-2 ACK. Without this, an invite-joiner forms a phantom single-node
	// meta-raft and never replays the leader's FSM (gen-0 DEK), so it times out
	// on WaitDEKReady. Mirror run.go's data-raft `joinLike` gating.
	joinLike := state.inviteJoinMode
	metaPeers := state.peers
	if joinLike {
		metaPeers = nil
	}
	metaRaft, err := cluster.NewMetaRaft(cluster.MetaRaftConfig{
		NodeID:   state.nodeID,
		RaftID:   state.raftAddr,
		Peers:    metaPeers,
		JoinMode: joinLike,
		DataDir:  state.cfg.DataDir,
		StoreOptions: cluster.RaftV2StoreOptions{
			EncryptionKey: state.raftStoreKey,
		},
	})
	if err != nil {
		return fmt.Errorf("init meta-raft: %w", err)
	}
	state.metaRaft = metaRaft
	state.capabilityGate = cluster.NewCapabilityGate(compat.DefaultRegistry, capabilityEvidenceTTL(state))
	state.metaRaft.SetCapabilityGate(state.capabilityGate)

	// Meta-raft RPCs go over transport.Call (StreamMetaRaft); inbound dispatch is
	// registered by the constructor. Receiver-side group dispatch stays on
	// groupRaftMux (StreamGroupRaft) independently.
	state.metaTransport = cluster.NewMetaTransport(state.clusterTransport, metaRaft.Node())
	metaRaft.SetTransport(state.metaTransport)

	// Phase 2 IAM: wire IAM store + applier into the meta-FSM apply path.
	// SetIAM is nil-safe for test configurations that do not provide IAM.
	if state.cfg.IAMStore != nil && state.cfg.IAMApplier != nil {
		metaRaft.FSM().SetIAM(state.cfg.IAMStore, state.cfg.IAMApplier)
	}
	// C2 §1 gap fix: construct the cluster DEK Keeper from the node KEK and
	// inject it into the FSM. Without this wireDEKKeeper call, DEKRotate /
	// DEKVersionPrune MetaCmds are silent no-ops at apply time. Extracted as a
	// function so the wiring contract is directly unit-testable (see
	// dek_keeper_wiring_test.go::TestWireDEKKeeper_InjectsAndRegistersHook).
	if err := wireDEKKeeper(state, metaRaft.FSM()); err != nil {
		return err
	}
	wireRaftStoreKeyPostCommit(state, metaRaft.FSM())

	// Zero-CA §6 D-rev3 step 1: persist a per-node transport identity for EVERY
	// member now that the KEK store + clusterID are wired. This seals
	// keys.d/node.key.enc once (genesis/normal boot included) and reloads it on
	// later boots, recording the per-node SPKI. The node does NOT yet present
	// this identity — accept-side foundation only. Task 6 consumes perNodeSPKI.
	// Skipped when identity inputs are missing. node.key.enc is sealed under the
	// active KEK generation and tracked by keys.d/node.key.gen so normal boot can
	// re-seal during rotation and fail closed after prune.
	if len(state.clusterID) > 0 && state.nodeID != "" {
		if state.inviteJoinMode {
			// Invite-join OWNS node.key.enc this boot: Phase-1 sealed it under a KEK
			// generation and Phase-2 (bootInviteJoinPhase2) LoadNodeKeys it under that
			// SAME gen. ensureNodeIdentity must NOT touch it here; Phase-2 owns the
			// active-generation re-seal at close-out (loadAndMigrateInviteNodeKey).
			// Self-register still needs the
			// SPKI, so source it from the invite-join state (set in Phase-1 / from the
			// resume sentinel).
			if state.inviteJoin != nil {
				state.perNodeSPKI = state.inviteJoin.nodeSPKI
			}
		} else {
			cert, spki, nodeKeyKEKGen, err := ensureNodeIdentity(
				state.cfg.DataDir,
				hex.EncodeToString(state.clusterID),
				state.nodeID,
				state.kekStore,
			)
			if err != nil {
				return fmt.Errorf("ensure per-node transport identity: %w", err)
			}
			state.perNodeCert = cert
			state.perNodeSPKI = spki
			state.perNodeKeyKEKGen = nodeKeyKEKGen
		}
	}

	// T25.5: wire IAM policy stores + resolver + builtin seed into the meta-FSM.
	// Must run before bootMetaRaftStart so apply hooks for MetaCmds 50-61 land
	// on the same store instances that authz (T26) will read from.
	iamStores, err := WireIAMPolicyStores(context.Background(), metaRaft.FSM(), 0)
	if err != nil {
		return fmt.Errorf("wire IAM policy stores: %w", err)
	}
	state.iamPolicyStores = iamStores

	protocolCredentialStore := protocred.NewStore()
	metaRaft.FSM().SetProtocolCredentialStore(protocolCredentialStore)
	state.protocolCredentialStore = protocolCredentialStore

	// T33: construct + wire the cluster config store.
	// T39: JWT signing-key rotate/prune triggers are wired here so that a
	// cluster-config PATCH to jwt.signing-key-rotate / jwt.signing-key-prune
	// propagates the MetaCmd to the meta-raft FSM on every node.
	cfgStore := config.NewStore()
	hooks := wireJWTReloadHooks(metaRaft, state.dekKeeper)

	config.RegisterClusterKeys(cfgStore, hooks)
	metaRaft.FSM().SetConfigStore(cfgStore)
	state.cfgStore = cfgStore

	// Task 10: register meta policy-invalidation post-commit hook BEFORE Start.
	// The worker is started here so it is ready before the apply loop fires.
	// SetInvalidate is called later in bootHTTPServerAndAdmin once the compiled
	// policy store (srv.PolicyStore()) is available. Events arriving before
	// SetInvalidate are silently dropped (pull-on-miss ensures eventual consistency).
	policyWorker := cluster.NewMetaPolicyInvalidationWorker()
	policyWorker.Start()
	state.AddCleanup(policyWorker.Stop)
	metaRaft.FSM().RegisterPostCommit(policyWorker.Hook)
	state.metaPolicyInvalidationWorker = policyWorker

	return nil
}

func refreshCapabilityGate(state *bootState) {
	if state == nil || state.capabilityGate == nil || state.metaRaft == nil {
		return
	}
	state.capabilityGate.SetTTL(capabilityEvidenceTTL(state))
	state.capabilityGate.SetMetaRaftSnapshot(state.metaRaft.Node().CommittedIndex(), state.metaRaft.Node().Configuration())
	state.capabilityGate.ReportEvidence(state.metaRaft.FSM().CapabilityEvidence(state.metaRaft.Node().ID(), time.Now()))
}

func capabilityEvidenceTTL(state *bootState) time.Duration {
	ttl := 15 * time.Second
	if state == nil || state.metaRaft == nil {
		return ttl
	}
	interval := state.metaRaft.FSM().ClusterConfig().BalancerGossipInterval()
	if interval > 0 && 3*interval > ttl {
		return 3 * interval
	}
	return ttl
}

// bootDataGroupRouter constructs the DataGroupManager + Router and registers
// the OnBucketAssigned callback on the meta-FSM. MUST run BEFORE
// bootMetaRaftStart so the callback is in place before the apply loop fires
// — otherwise the first bucket-assignment apply would race the SetOnBucketAssigned
// call (SetOnBucketAssigned takes f.mu.Lock() internally; Start releases the
// apply goroutine that also takes that lock).
func bootDataGroupRouter(state *bootState) error {
	state.dgMgr = cluster.NewDataGroupManager()
	state.clusterRouter = cluster.NewRouter(state.dgMgr)

	// SetOnBucketAssigned/SetOnBucketUnassigned use f.mu.Lock() internally; must be called
	// before Start() (which is bootMetaRaftStart's job).
	router := state.clusterRouter
	state.metaRaft.FSM().SetOnBucketAssigned(func(bucket, groupID string) {
		router.AssignBucket(bucket, groupID)
	})
	state.metaRaft.FSM().SetOnBucketUnassigned(func(bucket string) {
		router.Unassign(bucket)
	})
	return nil
}

// bootRotationAndAdminAPI registers the cluster-key rotation worker callbacks
// on the meta-FSM and constructs the IAM AdminAPI (when IAM is configured).
//
// presentFlipTarget narrows the cluster transport surface for the onPresentFlip
// callback — keeps the wiring testable with a small fake (PR-2a §8c step 5).
type presentFlipTarget interface {
	FlipPresent(cert tls.Certificate, spki [32]byte)
}

// presentsPerNodeWriter is the consumer-side slice of *cluster.MetaRaft used
// by onPresentFlip to mark this node as presenting its per-node cert.
type presentsPerNodeWriter interface {
	ProposeRegisterMember(ctx context.Context, nodeID string, spki [32]byte, addr string, presentsPerNode bool, nodeKeyKEKGen uint32) error
}

// buildOnPresentFlipCallbackWithRegistrar returns a callback that flips the
// transport's presented cert, then best-effort registers presentsPerNode=true.
func buildOnPresentFlipCallbackWithRegistrar(st *bootState, tr presentFlipTarget, reg presentsPerNodeWriter) func() {
	if tr == nil {
		return nil
	}
	return func() {
		if st.perNodeCert.Certificate == nil {
			// perNodeCert is nil in invite-join mode until bootInviteJoinPhase2
			// sets it. In normal operation, BeginPresentFlip can only be applied
			// AFTER the node is a voter (Phase 2 complete), so this branch
			// should not trigger. If it does, the cluster-wide flip proceeds but
			// THIS node still presents the old CA cert — operators must re-run
			// the cutover or restart the node.
			log.Error().Msg("onPresentFlip: perNodeCert not loaded at flip time — node will not flip; restart to recover")
			return
		}
		tr.FlipPresent(st.perNodeCert, st.perNodeSPKI)
		if reg == nil {
			return
		}
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := reg.ProposeRegisterMember(ctx, st.nodeID, st.perNodeSPKI, st.raftAddr, true, st.perNodeKeyKEKGen); err != nil {
				log.Warn().Err(err).Msg("onPresentFlip: presentsPerNode registration failed (non-fatal); D-cut4 gate may be delayed")
			}
		}()
	}
}

// clusterKeyDropTarget narrows the cluster transport surface for the
// onClusterKeyDropped callback.
type clusterKeyDropTarget interface {
	SetDropped()
	RecycleConns()
}

// buildOnClusterKeyDroppedCallback drops the cluster-key base and recycles
// existing transport connections so peers re-handshake without the cluster key.
func buildOnClusterKeyDroppedCallback(tr clusterKeyDropTarget) func() {
	if tr == nil {
		return nil
	}
	return func() {
		tr.SetDropped()
		tr.RecycleConns()
		log.Info().Msg("zero-CA: cluster_key_dropped applied; transport base dropped, conns recycled")
	}
}

// Like bootDataGroupRouter, this MUST run BEFORE bootMetaRaftStart: the apply
// loop must not fire any RotationApplied event before the worker callback is
// registered, or the first phase change is lost.
//
// AdminAPI is included here (rather than its own phase) because it is a
// thin wrapper over metaRaft.Propose — its construction has no Start
// dependency but it shares the IAM gating with this phase, so co-locating
// keeps the conditional check in one place.
func bootRotationAndAdminAPI(state *bootState) error {
	ks, err := newClusterKeystore(state.cfg.DataDir, state.cfg)
	if err != nil {
		return err
	}
	state.rotationKeystore = ks
	state.rotationWorker = cluster.NewRotationWorker(state.rotationKeystore, state.clusterTransport, state.nodeID)
	worker := state.rotationWorker
	state.metaRaft.FSM().SetOnRotationApplied(func(st cluster.RotationState) {
		_ = worker.OnPhaseChange(st)
	})
	state.metaRaft.FSM().SetOnPeersChanged(func(accept [][32]byte) {
		// Feed the peer-registry SPKIs into the transport's identity composer as
		// a delta. The composer recomputes base PSK ∪ rotation window ∪ registry,
		// so the registry never clobbers the steady-state PSK SPKI or a live
		// rotation window (spec §6 D-rev3 step 3).
		state.clusterTransport.UpdateRegistryAccept(accept)
	})
	// Persisted drop bit (spec §8 H3): if a restored snapshot says the cluster
	// key was dropped (a PR-2 feature; ALWAYS false in PR-1), drop the
	// cluster-key base on this node too. Dormant in PR-1 — no snapshot carries true.
	state.metaRaft.FSM().SetOnClusterKeyDropped(buildOnClusterKeyDroppedCallback(state.clusterTransport))
	// PR-2a §8c step 5: lazy present-flip — FlipPresent only, no RecycleConns.
	if cb := buildOnPresentFlipCallbackWithRegistrar(state, state.clusterTransport, state.metaRaft); cb != nil {
		state.metaRaft.FSM().SetOnPresentFlip(func() {
			cb()
			log.Info().Msg("present-flip applied: transport now presents per-node cert; presentsPerNode registered")
		})
	}
	// PR-2a §8b: applied-index probe handler for the leader's barrier fan-out.
	// Native /probe/applied-index buffered route. A bad request maps to a 500
	// → client error, matching the tunnel's nil-response StatusError.
	appliedIndexHandler := func(payload []byte) ([]byte, error) {
		respPayload, err := cluster.HandleAppliedIndexProbe(payload, state.nodeID, state.metaRaft.LastApplied)
		if err != nil {
			log.Warn().Err(err).Msg("applied-index probe: bad request")
			return nil, err
		}
		return respPayload, nil
	}
	state.clusterTransport.RegisterBufferedRoute(transport.RouteProbeAppliedIndex, appliedIndexHandler)
	// Seed rotation FSM steady state with active SPKI so RotateKeyBegin can
	// be validated against the current cluster key (D10).
	if _, activeSPKI, err := transport.DeriveClusterIdentity(state.transportPSK); err == nil {
		state.metaRaft.FSM().SetRotationSteady(activeSPKI)
	} else {
		log.Warn().Err(err).Msg("failed to seed rotation FSM steady state; rotation will be unavailable until next restart")
	}

	// Build the AdminAPI wired against the meta-FSM proposer. Only when IAM
	// dependencies are wired. First-SA bootstrap is performed via admin UDS
	// POST /v1/iam/sa (see docs/operators/runbook.md).
	//
	// R2: NewAdminAPI is constructed with a nil DataEncryptor; the live
	// DEKKeeperAdapter is wired by wireIAMEncryptor(state) below once the
	// DEK keeper is at its final value (after any restore-time reassignment).
	if state.cfg.IAMStore != nil && state.cfg.IAMApplier != nil {
		state.iamProposer = &iam.MetaProposer{Propose: state.metaRaft.Propose}
		state.iamAdminAPI = iam.NewAdminAPI(state.cfg.IAMStore, state.iamProposer, nil)
	}

	// R2: install the live DEKKeeperAdapter into BOTH the IAM applier and
	// admin API now that the DEK keeper is wired and the AdminAPI exists.
	// Idempotent — restore-branch may reassign state.dekKeeper later (see
	// dek_keeper_restore.go), which fires wireIAMEncryptor again with the
	// restored keeper.
	wireIAMEncryptor(state)
	return nil
}

// bootMetaRaftStart fires the meta-raft apply loop. After this returns, every
// FSM callback registered by bootDataGroupRouter and bootRotationAndAdminAPI
// is live and apply events flow into them. Bootstrap (when not in join mode)
// runs first so a single-node cluster can elect a leader.
//
// Post-Start init: previous-key cleanup goroutine, rotation socket, Close
// cleanup. Router.Sync(BucketAssignments) is also done here — the meta-FSM
// finishes replay inside Start, so calling Sync afterwards seeds the router
// with all buckets persisted before Start returned.
//
// The rotation-socket starter is read from state.startRotationSocket (a
// bootState field, defaulted to StartRotationSocket in Run; nil in tests that
// exercise the phase without a real admin UDS). Keeping it a field rather than a
// parameter lets this phase fit the uniform bootSequence() signature while
// preserving the test-override seam.
func bootMetaRaftStart(ctx context.Context, state *bootState) error {
	// An invite-joiner (inviteJoinMode) must NOT bootstrap its own meta-raft —
	// same as joinMode, the leader adds it as a voter via the Phase-2 ACK.
	if !state.inviteJoinMode {
		if err := state.metaRaft.Bootstrap(); err != nil {
			return fmt.Errorf("meta-raft bootstrap: %w", err)
		}
	}
	// §7 T57: the preApplyLoop callback runs AFTER Restore (so the DKVS
	// trailer is decoded into FSM.PendingDEKVersions) and BEFORE the apply
	// loop is launched. This is the only safe window to swap the DEKKeeper
	// without racing DEKRotate / JWTSigningKeyRotate apply entries. On a
	// fresh boot (no snapshot trailer) the callback is a no-op.
	preApply := func() error {
		return rebuildDEKKeeperFromRestore(state, state.metaRaft.FSM())
	}
	if err := state.metaRaft.Start(ctx, preApply); err != nil {
		return fmt.Errorf("meta-raft start: %w", err)
	}
	// Register the live KEK Prometheus collector now that state.dekKeeper is
	// final (rebuildDEKKeeperFromRestore may have swapped it during preApply).
	// The collector reads the keeper + lease tracker + FSM lifecycle table at
	// scrape time, so grainfs_kek_seal_count is scrape-fresh on /metrics and
	// grainfs_kek_retired_count agrees with the admin status JSON. No-op when
	// encryption is disabled (nil keeper).
	if state.dekKeeper != nil {
		metrics.RegisterKEKCollector(state.dekKeeper, state.kekLeaseTracker, state.metaRaft.FSM())
	}
	// previous.key cleanup goroutine — deletes keys.d/previous.key after
	// RotationPreviousGrace expires. Runs on all nodes (FSM state is
	// identical via raft); each node deletes its own local file.
	state.metaRaft.StartPreviousKeyCleanup(ctx, state.rotationKeystore)
	if state.startRotationSocket != nil {
		if err := state.startRotationSocket(ctx, state.cfg.DataDir, state.cfg, state.metaRaft); err != nil {
			log.Warn().Err(err).Msg("rotation socket failed to start; cluster rotate-key CLI will be unavailable")
		}
	}
	state.AddCleanup(func() { state.metaRaft.Close() })

	// Seed Router with bucket assignments already persisted in FSM state.
	// Start() returns before replay finishes; onBucketAssigned (registered
	// in bootDataGroupRouter) fires live updates, and this Sync covers any
	// assignments applied during the synchronous replay window.
	state.clusterRouter.Sync(state.metaRaft.FSM().BucketAssignments())
	return nil
}

// genesisBootstrapDEKWait bounds how long the genesis node waits to win its
// (single-voter) election before proposing gen-0. Genesis elects in ~1 heartbeat
// tick; a few seconds is generous slack while still surfacing a real bug if the
// node never becomes leader.
const genesisBootstrapDEKWait = 10 * time.Second

// bootGenesisDEKBootstrap replicates the genesis node's locally-generated DEK
// gen-0 through the UNGATED bootstrap propose (Phase D Task 5) so joining nodes
// install identical bytes via log replay / snapshot restore.
//
// Fires EXACTLY ONCE in the cluster's lifetime, on the original fresh-init boot:
//
//   - Never on a joiner / node with static peers: isGenesisBoot is false, and
//     such a node holds an EMPTY keeper anyway (nothing to propose).
//   - Never on a restart of a former-genesis node: priorState makes
//     isGenesisBoot false; gen-0 is reinstalled by rebuildDEKKeeperFromRestore.
//   - Even a stray duplicate is a deterministic no-op: ProposeDEKBootstrap
//     rejects gen != 0 and applyDEKReplicatedRotate's bootstrap sentinel only
//     installs when no DEK gen exists yet (belt-and-suspenders).
//
// The propose is UNGATED by design: at this moment the genesis node is the sole
// voter, so the all-voter dek_replicated_v1 gate cannot pass (followers have not
// gossiped capability evidence yet — the gen-0 paradox). Propose forwards to the
// leader when not leader, so we wait for this node to win its single-voter
// election before proposing.
func bootGenesisDEKBootstrap(ctx context.Context, state *bootState) error {
	if !isGenesisBoot(state) || state.dekKeeper == nil {
		return nil
	}
	waitCtx, cancel := context.WithTimeout(ctx, genesisBootstrapDEKWait)
	defer cancel()
	tick := time.NewTicker(cluster.MetaRaftHeartbeatInterval)
	defer tick.Stop()
	for !state.metaRaft.IsLeader() {
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("bootGenesisDEKBootstrap: genesis node did not win leadership within %s: %w", genesisBootstrapDEKWait, waitCtx.Err())
		case <-tick.C:
		}
	}
	versions, active := state.dekKeeper.VersionsAndActive() // active=0, gen-0 present
	kekVer := state.kekStore.ActiveVersion()
	if err := state.metaRaft.ProposeDEKBootstrap(ctx, active, versions[active], kekVer); err != nil {
		return fmt.Errorf("bootGenesisDEKBootstrap: replicate gen-0: %w", err)
	}
	return nil
}
