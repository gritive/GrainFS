package serveruntime

import (
	"context"
	"fmt"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/iam"
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

	// Mux-aware constructor: auto-registers metaRaft.Node() on groupRaftMux
	// under the magic groupID "__meta__" so receiver-side mux dispatch is
	// wired before any meta heartbeat hits the wire.
	state.metaTransport = cluster.NewMetaTransportQUICMux(state.quicTransport, metaRaft.Node(), state.groupRaftMux)
	metaRaft.SetTransport(state.metaTransport)

	// Phase 2 IAM: wire IAM store + applier into the meta-FSM apply path.
	// SetIAM is nil-safe for iamApplier (--no-encryption mode).
	if state.cfg.IAMStore != nil && state.cfg.IAMApplier != nil {
		metaRaft.FSM().SetIAM(state.cfg.IAMStore, state.cfg.IAMApplier)
	}
	// Cluster-config PATCH with alert-webhook-secret requires the encryptor on
	// the FSM (apply-side gate, Task 7). Nil in --no-encryption mode — Apply
	// then rejects such patches with "encryption disabled".
	if state.cfg.Encryptor != nil {
		metaRaft.FSM().SetEncryptor(state.cfg.Encryptor)
	}
	return nil
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
	// Seed rotation FSM steady state with active SPKI so RotateKeyBegin can
	// be validated against the current cluster key (D10).
	if _, activeSPKI, err := transport.DeriveClusterIdentity(state.transportPSK); err == nil {
		state.metaRaft.FSM().SetRotationSteady(activeSPKI)
	} else {
		log.Warn().Err(err).Msg("failed to seed rotation FSM steady state; rotation will be unavailable until next restart")
	}

	// Build the AdminAPI wired against the meta-FSM proposer. Only when IAM
	// dependencies are wired. First-SA bootstrap is performed via admin UDS
	// POST /v1/iam/sa (see docs/RUNBOOK.md).
	if state.cfg.IAMStore != nil && state.cfg.IAMApplier != nil && state.cfg.Encryptor != nil {
		state.iamProposer = &iam.MetaProposer{Propose: state.metaRaft.Propose}
		state.iamAdminAPI = iam.NewAdminAPI(state.cfg.IAMStore, state.iamProposer, state.cfg.Encryptor)
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
	if err := state.metaRaft.Start(ctx); err != nil {
		return fmt.Errorf("meta-raft start: %w", err)
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
