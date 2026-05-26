package serveruntime

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/compat"
	"github.com/gritive/GrainFS/internal/config"
	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/iam/mountsastore"
	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/nfsexport"
	"github.com/gritive/GrainFS/internal/nodeconfig"
	"github.com/gritive/GrainFS/internal/server"
	"github.com/gritive/GrainFS/internal/transport"
)

// bootMetaRaftWiring constructs the meta-raft control plane and its QUIC
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
	metaPeers := state.peers
	if state.joinMode {
		metaPeers = nil
	}
	metaRaft, err := cluster.NewMetaRaft(cluster.MetaRaftConfig{
		NodeID:   state.nodeID,
		RaftID:   state.raftAddr,
		Peers:    metaPeers,
		JoinMode: state.joinMode,
		DataDir:  state.cfg.DataDir,
	})
	if err != nil {
		return fmt.Errorf("init meta-raft: %w", err)
	}
	state.metaRaft = metaRaft
	state.capabilityGate = cluster.NewCapabilityGate(compat.DefaultRegistry, capabilityEvidenceTTL(state))
	state.metaRaft.SetCapabilityGate(state.capabilityGate)

	// Mux-aware constructor: auto-registers metaRaft.Node() on groupRaftMux
	// under the magic groupID "__meta__" so receiver-side mux dispatch is
	// wired before any meta heartbeat hits the wire.
	state.metaTransport = cluster.NewMetaTransportQUICMux(state.quicTransport, metaRaft.Node(), state.groupRaftMux)
	metaRaft.SetTransport(state.metaTransport)

	exportStore, err := nfsexport.OpenStore(state.db)
	if err != nil {
		return fmt.Errorf("open NFS export store: %w", err)
	}
	metaRaft.FSM().SetExportStore(exportStore)
	metaRaft.FSM().SetExportFsidMajor(1)
	state.nfsExportSvc = nfsexport.NewExportService(nfsexport.ServiceConfig{
		Store: exportStore,
		Proposer: &cluster.NfsExportProposer{
			Propose:         metaRaft.ProposeWithIndex,
			ProposeWithGate: metaRaft.ProposeWithGate,
			GatePlan: func(operation compat.Operation) (compat.GatePlan, error) {
				refreshCapabilityGate(state)
				return state.capabilityGate.RequireMetaRaftCapability(compat.CapabilityNfsExportCreateV1, operation, time.Now())
			},
		},
		Barrier: metaRaft,
	})

	// Phase 2 IAM: wire IAM store + applier into the meta-FSM apply path.
	// SetIAM is nil-safe for test configurations that do not provide IAM.
	if state.cfg.IAMStore != nil && state.cfg.IAMApplier != nil {
		metaRaft.FSM().SetIAM(state.cfg.IAMStore, state.cfg.IAMApplier)
	}
	// Cluster-config PATCH with alert-webhook-secret requires the encryptor on
	// the FSM (apply-side gate, Task 7). Nil test configurations reject such
	// patches with "encryption disabled".
	if state.cfg.Encryptor != nil {
		metaRaft.FSM().SetEncryptor(state.cfg.Encryptor)
	}

	// C2 §1 gap fix: construct the cluster DEK Keeper from the node KEK and
	// inject it into the FSM. Without this wireDEKKeeper call, DEKRotate /
	// DEKVersionPrune MetaCmds are silent no-ops at apply time. Extracted as a
	// function so the wiring contract is directly unit-testable (see
	// dek_keeper_wiring_test.go::TestWireDEKKeeper_InjectsAndRegistersHook).
	if err := wireDEKKeeper(state, metaRaft.FSM()); err != nil {
		return err
	}

	// T25.5: wire IAM policy stores + resolver + builtin seed into the meta-FSM.
	// Must run before bootMetaRaftStart so apply hooks for MetaCmds 50-61 land
	// on the same store instances that authz (T26) will read from.
	iamStores, err := WireIAMPolicyStores(context.Background(), metaRaft.FSM(), 0)
	if err != nil {
		return fmt.Errorf("wire IAM policy stores: %w", err)
	}
	state.iamPolicyStores = iamStores

	// NFS§A T5: wire the MountSA Badger-backed store into the meta-FSM apply path.
	// MetaCmds 65-68 (MountSACreate/Delete/AttachPolicy/DetachPolicy) require
	// the store to be wired before Start fires the apply loop.
	mountSAStore, err := mountsastore.NewStore(state.db)
	if err != nil {
		return fmt.Errorf("open mountsa store: %w", err)
	}
	metaRaft.FSM().SetMountSAStore(mountSAStore)
	state.mountSAStore = mountSAStore

	// T33: construct + wire the cluster config store. This is the §1 gap
	// (previously deferred) — needed so s3auth.Authorizer can read iam.anon-enabled
	// at request time.
	// T39: JWT signing-key rotate/prune triggers are wired here so that a
	// cluster-config PATCH to jwt.signing-key-rotate / jwt.signing-key-prune
	// propagates the MetaCmd to the meta-raft FSM on every node.
	cfgStore := config.NewStore()
	hooks := wireJWTReloadHooks(metaRaft, state.dekKeeper)
	// §5 T44: refuse runtime flips into an unsafe TLS posture. config.Store.Set
	// rolls back on hook error, so a `grainfs config set iam.anon-enabled false`
	// is rejected atomically when no cert + no trusted proxy is configured.
	// The hook fires under the store's write lock, so it MUST NOT re-query
	// cfgStore — trusted-proxy.cidr is tracked in an atomic snapshot kept
	// fresh by a sibling OnTrustedProxyCIDR hook.
	onAnon, onProxy, refreshProxy := wireTLSPostureHooks("")

	// §5 T45: construct the ProxyTrust validator and wrap onProxy so a single
	// OnTrustedProxyCIDR firing updates BOTH (a) the TLS-posture atomic
	// snapshot used by the anon-change hook and (b) the live CIDR set used by
	// (*Server).authoritativeClientIP. ReloadHooks.OnTrustedProxyCIDR is
	// single-slot (one func), so we compose at the wire site rather than
	// touching the hook plumbing.
	proxyTrust := server.NewProxyTrust(nil)
	state.proxyTrust = proxyTrust
	// §5 T46: wrap the posture-check hook so the operator gets a one-shot
	// "s3://default remains public" INFO banner on a successful true→false
	// flip. Initial value is the registered default (true) — anon-enabled
	// has not yet been Set at wire time, so the BoolSpec default is the
	// correct seed. state.bannerWriter is os.Stdout in production (set in
	// Run); tests that route through bootstrap.Run can substitute a buffer
	// before phase dispatch.
	hooks.OnAnonEnabledChange, state.anonBannerSeedPrev = composeAnonHookWithBanner(onAnon, true, state.bannerWriter)
	hooks.OnTrustedProxyCIDR = func(ctx context.Context, v string) error {
		proxyTrust.SetCIDRs(splitTrustedProxyCIDRSpec(v))
		return onProxy(ctx, v)
	}
	state.refreshProxyCIDR = refreshProxy

	// §6 T52': route audit.deny-only reloads to the audit outbox.
	// The outbox is constructed later in boot_phases_srvopts, so the closure
	// reads through state.auditOutbox at fire time (nil-safe). When audit
	// iceberg is disabled, state.auditOutbox stays nil and the hook is a
	// silent no-op — consistent with the config key's BoolSpec default of
	// false (no operator-visible flip happens at boot).
	hooks.OnAuditDenyOnly = func(_ context.Context, v bool) error {
		state.auditOutbox.SetDenyOnly(v) // Outbox.SetDenyOnly is nil-safe
		return nil
	}

	config.RegisterClusterKeys(cfgStore, hooks)
	// F25+F26: fire a post-restore callback so atomic snapshots (proxy CIDR set
	// and banner-prev) are reconciled on every raft InstallSnapshot. Restore does
	// not fire reload hooks, so without this the ProxyTrust CIDR set and the
	// banner-prev bool would drift from the newly restored cfgStore values on
	// peer-join and log-compaction restores after boot.
	cfgStore.SetPostRestore(func(values map[string]string) {
		// F25: update ProxyTrust and the TLS-posture refreshProxy snapshot.
		cidr := values["trusted-proxy.cidr"]
		proxyTrust.SetCIDRs(splitTrustedProxyCIDRSpec(cidr))
		refreshProxy(cidr)
		// F26: re-seed banner-prev so the next OnAnonEnabledChange hook firing
		// compares against the restored value, not the stale wire-time seed.
		if state.anonBannerSeedPrev != nil {
			anonEnabled := true // matches BoolSpec default for iam.anon-enabled
			if v, ok := values["iam.anon-enabled"]; ok {
				anonEnabled = v == "true"
			}
			state.anonBannerSeedPrev(anonEnabled)
		}
	})
	metaRaft.FSM().SetConfigStore(cfgStore)
	state.cfgStore = cfgStore
	return nil
}

// splitTrustedProxyCIDRSpec splits the comma-separated trusted-proxy.cidr value
// into entries suitable for ProxyTrust.SetCIDRs. Empty entries are tolerated;
// ProxyTrust.SetCIDRs additionally trims and silently drops invalid CIDRs.
func splitTrustedProxyCIDRSpec(v string) []string {
	if v == "" {
		return nil
	}
	return strings.Split(v, ",")
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
	state.clusterRouter.SetDefault("group-0")

	// SetOnBucketAssigned uses f.mu.Lock() internally; must be called
	// before Start() (which is bootMetaRaftStart's job).
	router := state.clusterRouter
	state.metaRaft.FSM().SetOnBucketAssigned(func(bucket, groupID string) {
		router.AssignBucket(bucket, groupID)
	})
	return nil
}

// bootRotationAndAdminAPI registers the cluster-key rotation worker callbacks
// on the meta-FSM and constructs the IAM AdminAPI (when IAM is configured).
//
// Like bootDataGroupRouter, this MUST run BEFORE bootMetaRaftStart: the apply
// loop must not fire any RotationApplied event before the worker callback is
// registered, or the first phase change is lost.
//
// AdminAPI is included here (rather than its own phase) because it is a
// thin wrapper over metaRaft.Propose — its construction has no Start
// dependency but it shares the IAM gating with this phase, so co-locating
// keeps the conditional check in one place.
func bootRotationAndAdminAPI(state *bootState) error {
	state.rotationKeystore = transport.NewKeystore(state.cfg.DataDir)
	state.rotationWorker = cluster.NewRotationWorker(state.rotationKeystore, state.quicTransport, state.nodeID)
	worker := state.rotationWorker
	state.metaRaft.FSM().SetOnRotationApplied(func(st cluster.RotationState) {
		_ = worker.OnPhaseChange(st)
	})
	// Rebuild the transport accept-set live after every peer-registry change so
	// the listener immediately accepts the new peer set (Phase 1 GetConfigForClient
	// reads identity.Load() per handshake).
	//
	// TODO(phase-2-followup): membership vs rotation accept-set union. A concurrent
	// KEK rotation also drives SwapIdentity with a 2-SPKI window ([OldSPKI,NewSPKI]);
	// a membership change during a rotation would overwrite that window with the
	// registry accept set. Union-of-both is out of Phase 2 scope.
	state.metaRaft.FSM().SetOnPeersChanged(func(accept [][32]byte) {
		state.quicTransport.SwapIdentity(state.quicTransport.IdentitySnapshotForAccept(accept))
	})
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
	if state.cfg.IAMStore != nil && state.cfg.IAMApplier != nil && state.cfg.Encryptor != nil {
		state.iamProposer = &iam.MetaProposer{Propose: state.metaRaft.Propose}
		state.iamAdminAPI = iam.NewAdminAPI(state.cfg.IAMStore, state.iamProposer, state.cfg.Encryptor)
		// F#26-tls-posture: gate the first SA create on local TLS posture so
		// the admin UDS rejects with a remediation hint instead of letting
		// the FSM-level anon flip silently fail. cfgStore is populated by
		// bootHTTPServerAndAdmin which runs before this phase.
		if state.cfgStore != nil {
			state.iamAdminAPI.SetPostureChecker(
				newIAMPostureChecker(state.cfgStore, nodeconfig.New(state.cfg.DataDir)),
			)
		}
	}
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
// startRotationSocket is plumbed via parameter to keep this phase testable
// without a real admin UDS. Production callers pass StartRotationSocket;
// tests can pass a no-op.
func bootMetaRaftStart(ctx context.Context, state *bootState, startRotationSocket func(context.Context, string, *cluster.MetaRaft) error) error {
	if !state.joinMode {
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
	if startRotationSocket != nil {
		if err := startRotationSocket(ctx, state.cfg.DataDir, state.metaRaft); err != nil {
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
