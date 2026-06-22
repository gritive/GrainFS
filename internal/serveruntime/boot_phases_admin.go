package serveruntime

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	hzserver "github.com/cloudwego/hertz/pkg/app/server"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/dashboard"
	"github.com/gritive/GrainFS/internal/iam/pdp"
	"github.com/gritive/GrainFS/internal/lifecycle"
	"github.com/gritive/GrainFS/internal/nodeconfig"
	"github.com/gritive/GrainFS/internal/protocred"
	"github.com/gritive/GrainFS/internal/s3auth"
	"github.com/gritive/GrainFS/internal/server"
	"github.com/gritive/GrainFS/internal/server/admin"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/transport"
)

// CompleteCutoverHandler handles POST /v1/cluster/complete-cutover.
type CompleteCutoverHandler struct {
	RunDrop func(ctx context.Context) error
}

func (h *CompleteCutoverHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h == nil || h.RunDrop == nil {
		writeCompleteCutoverJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "complete-cutover unavailable"})
		return
	}
	if err := h.RunDrop(r.Context()); err != nil {
		code := http.StatusInternalServerError
		msg := err.Error()
		if strings.Contains(msg, "D-cut4") ||
			strings.Contains(msg, "single-node") ||
			strings.Contains(msg, "empty voter") ||
			strings.Contains(msg, "not in peer registry") {
			code = http.StatusBadRequest
		}
		writeCompleteCutoverJSON(w, code, map[string]string{"error": msg})
		return
	}
	writeCompleteCutoverJSON(w, http.StatusOK, map[string]string{
		"status":  "ok",
		"message": "cluster key dropped",
	})
}

func writeCompleteCutoverJSON(w http.ResponseWriter, code int, body map[string]string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

// bootHTTPServerAndAdmin constructs the data-plane server.Server, wires the
// dashboard token middleware + admin UI routes onto its Hertz engine, and
// opens the admin Unix Domain Socket. Spec A5 invariant: admin routes are
// registered BEFORE the data-plane Hertz starts serving.
//
// Inputs:  state.cfg.Addr, state.backend, state.srvOpts,
//
//	state.cfg.DataDir, state.cfg.PublicURL, state.nodeID,
//	state.distBackend, state.cfg.VlogWatchEnabled/* ratios */,
//	state.metaRaft, state.cfg.AdminSocket/AdminGroup,
//	state.iamAdminAPI.
//
// Outputs: state.srv, state.tokenStore, state.adminDeps, state.adminSrv.
//
// Cleanup: best-effort adminSrv.Stop is registered via state.AddCleanup so
// early returns still unlink the socket. The explicit ordered shutdown in
// bootShutdownDrain still runs first on the happy path; admin.Stop is
// idempotent.
func bootHTTPServerAndAdmin(state *bootState) error {
	cfg := state.cfg

	operatorCollector, operatorGatherer := newOperatorStateMetricsCollector(state)
	state.srvOpts = append(state.srvOpts, server.WithMetricsGatherer(operatorGatherer))
	srv := server.New(cfg.Addr, state.backend, state.srvOpts...)
	state.srv = srv

	// Wire the bucket-policy cache invalidator into the cluster apply path so a
	// committed policy change/delete on any node drops this node's compiled cache
	// entry, forcing the next authz Allow to re-pull the committed policy. The
	// loader (pull-on-miss) is already wired mode-agnostically in storage.NewOperations.
	if state.distBackend != nil {
		state.distBackend.SetOnBucketPolicyApply(srv.PolicyStore().Invalidate)
	}

	// --- Admin / dashboard wiring (Volume CLI Phase B) ---
	tokenStore, err := dashboard.Open(filepath.Join(cfg.DataDir, "dashboard.token"))
	if err != nil {
		return fmt.Errorf("dashboard token: %w", err)
	}
	state.tokenStore = tokenStore
	if state.iamAdminAPI != nil && state.cfgStore != nil {
		state.iamAdminAPI.SetPostureChecker(
			newIAMPostureChecker(state.cfgStore, nodeconfig.New(state.cfg.DataDir)),
		)
	}
	if state.protocolCredentials == nil {
		if state.metaRaft != nil {
			ensureProtocolCredentialStore(state)
			state.protocolCredentials = cluster.NewProtocolCredentialService(
				state.protocolCredentialStore,
				state.metaRaft.Propose,
				cluster.WithProtocolCredentialSecretEnvelope(protocolCredentialEnvelopeFromState(state)),
			)
		} else {
			ensureProtocolCredentialStore(state)
			state.protocolCredentials = protocred.NewService(
				state.protocolCredentialStore,
				protocred.WithSecretEnvelope(protocolCredentialEnvelopeFromState(state)),
			)
		}
	}
	state.adminDeps = &admin.Deps{
		Token:      tokenStore,
		PublicURL:  cfg.PublicURL,
		NodeID:     state.nodeID,
		PeerHealth: NewPeerHealthAdapter(state.distBackend),
		VlogBreakdown: NewVlogBreakdownAdapter(VlogBreakdownOptions{
			Enabled:       cfg.VlogWatchEnabled,
			DataDir:       cfg.DataDir,
			WarnRatio:     cfg.VlogWarnRatio,
			CriticalRatio: cfg.VlogCriticalRatio,
		}),
		IAM:                      state.iamAdminAPI,
		IcebergConfig:            newIcebergConfigAdapter(state.cfg.IAMStore),
		IAMPolicy:                iamPolicyAdminService(state),
		IAMGroup:                 iamGroupAdminService(state),
		BucketWithPolicyProp:     bucketWithPolicyProposer(state),
		LifecycleDeleteProp:      lifecycleDeleteProposer(state),
		LifecycleGenReader:       lifecycleGenReader(state),
		BucketUpstreamDeleteProp: bucketUpstreamDeleteProposer(state),
		ConfigProposer:           state.metaRaft,
		ConfigStore:              state.cfgStore,
		Buckets:                  storage.NewOperations(state.backend),
		ProtocolCredentials:      state.protocolCredentials,
		ProtocolCredAuthz:        protocolCredentialAuthorizer(state),
		AdminAuthz:               adminAuthorizer(state, "admin"),
		ActorAuth:                newOIDCActorAuthenticator(state.cfgStore),
		PDPTokens:                ensurePDPTokenSource(state),
	}
	if state.auditSearcher != nil {
		state.adminDeps.AuditQuery = state.auditSearcher
	}
	state.adminDeps.Status = NewStatusAdapter(
		state.nodeID,
		cfg.DataDir,
		NewPeerHealthAdapter(state.distBackend),
		state.iamAdminAPI,
		state.dekKeeper,
		state.metaRaft,
		state.cfgStore,
	)
	operatorCollector.SetSources(operatorStateSources(state))
	dataHertz := srv.HertzEngine()
	dataHertz.Use(server.DashboardTokenMiddleware(tokenStore))
	admin.RegisterUI(dataHertz, state.adminDeps)

	// Open the admin Unix socket. Operator commands (`grainfs iam *`, `grainfs
	// dashboard`) reach this socket; permissions are governed by the file mode
	// (0660) and optional --admin-group chown.
	adminSocket := cfg.AdminSocket
	if adminSocket == "" {
		adminSocket = filepath.Join(cfg.DataDir, "admin.sock")
	}
	adminSrv, err := admin.Start(admin.Config{
		SocketPath: adminSocket,
		Group:      cfg.AdminGroup,
		Deps:       state.adminDeps,
		ExtraRoutes: func(h *hzserver.Hertz) {
			registerTestEndpoints(h, state.placementStatsStore)
			srv.RegisterClusterAdminUDS(h)
			if state.metaRaft != nil {
				// Production proposer routes through MetaRaft.Propose, which
				// transparently leader-forwards on followers via the existing
				// forward path (see meta_raft.proposeOrForward).
				proposer := &cluster.ClusterConfigProposer{Propose: state.metaRaft.Propose}
				var clusterConfigSecretEnc storage.DataEncryptor
				if state.dekKeeper != nil {
					clusterConfigSecretEnc = storage.NewDEKKeeperAdapter(state.dekKeeper, state.clusterID)
				}
				RegisterClusterConfigRoutes(h, state.metaRaft.FSM(), proposer, clusterConfigSecretEnc)

				// KEK envelope admin endpoints (Task 11). Routes register
				// unconditionally so operators always see a well-formed 503
				// "kek admin disabled" when the leader/gate aren't wired,
				// instead of a generic 404. state.kekRotationLeader is nil
				// until the production wiring lands (Phase B Task 11 step 2),
				// at which point the handler proxies through to the leader.
				RegisterEncryptionKEKRoutes(h, state.kekRotationLeader, state.capabilityGate, state.metaRaft.FSM(), state.dekKeeper, state.kekLeaseTracker)

				// Zero-CA W10: invite mint + operator bundle. SeedAddr/SeedSPKI
				// come from the running join listener (W9a accessors on
				// bootState); cluster.id from the post-apply FSM.
				inviteH := &InviteHandler{
					proposer:     state.metaRaft,
					clusterID:    state.metaRaft.FSM(),
					joinListener: state,
				}
				h.POST("/v1/cluster/invite/create", inviteH.Handle)

				completeCutoverH := &CompleteCutoverHandler{
					RunDrop: func(ctx context.Context) error {
						dialer := func(ctx context.Context, peer string, payload []byte) ([]byte, error) {
							// Native /probe/applied-index buffered route (Phase 8
							// N7-3): a handler failure (the tunnel's nil-response
							// StatusError) surfaces as the call error.
							return state.clusterTransport.CallBuffered(ctx, peer, transport.RouteProbeAppliedIndex, payload)
						}
						return state.metaRaft.CompleteCutover(ctx, dialer, 60*time.Second)
					},
				}
				h.POST("/v1/cluster/complete-cutover", wrapStdlibNoParam(completeCutoverH.ServeHTTP))

				revokeNodeH := &RevokeNodeHandler{
					RunRevoke: func(ctx context.Context, nodeID string) error {
						return state.metaRaft.RevokeNode(ctx, nodeID, state.clusterTransport.ClosePeer)
					},
				}
				h.POST("/v1/cluster/revoke-node", wrapStdlibNoParam(revokeNodeH.ServeHTTP))
			}
		},
	})
	if err != nil {
		return fmt.Errorf("admin server: %w", err)
	}
	state.adminSrv = adminSrv
	log.Info().Str("path", adminSocket).
		Str("hint", fmt.Sprintf("--endpoint %q", adminSocket)).
		Msg("admin endpoint")

	// Best-effort fallback: ensures the socket is unlinked even if the explicit
	// shutdown path is bypassed (e.g. early return). The explicit shutdown
	// sequence in bootShutdownDrain stops admin BEFORE the data plane drains
	// so operator commands cannot land on a half-shutdown server (spec A5).
	state.AddCleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = adminSrv.Stop(stopCtx)
	})
	return nil
}

func protocolCredentialAuthorizer(state *bootState) admin.CredentialAuthorizer {
	return adminAuthorizer(state, "protocol_credential")
}

func adminAuthorizer(state *bootState, scope string) admin.CredentialAuthorizer {
	if state.iamPolicyStores == nil || state.iamPolicyStores.Resolver == nil || state.cfgStore == nil {
		return nil
	}
	base := s3auth.NewAuthorizer(state.iamPolicyStores.Resolver, state.cfgStore)
	// Always install the PDP decorator; it is a pure pass-through unless iam.pdp
	// is enabled (read per request from the cfg store), so hot-enable works with
	// no dependency rebuild.
	return pdp.NewDecorator(base, state.cfgStore, ensurePDPTokenSource(state), scope)
}

// ensurePDPTokenSource lazily constructs the single PDP TokenSource /
// admin.PDPTokenManager instance and caches it on state. It is called from the
// two adminAuthorizer sites (protocolCredentialAuthorizer + AdminAuthz) and the
// admin.Deps literal, so the nil-check collapses all three to one instance.
// Boot is single-threaded here, so the guard needs no locking; the source's
// atomic.Pointer only guards request-path reads vs. restore-swap writes.
//
// The encryptor is seeded immediately when the DEK keeper is already wired
// (normal boot: admin phase runs after the raft phase). wireIAMEncryptor
// re-pushes the live adapter on fresh boot and after snapshot-restore swaps.
func ensurePDPTokenSource(state *bootState) *pdpTokenSource {
	if state.pdpTokenSource == nil {
		state.pdpTokenSource = newPDPTokenSource(state.cfgStore)
		if state.dekKeeper != nil {
			state.pdpTokenSource.setEncryptor(storage.NewDEKKeeperAdapter(state.dekKeeper, state.clusterID))
		}
	}
	return state.pdpTokenSource
}

// iamPolicyAdminService returns a wired admin.IAMPolicyService if MetaRaft and
// IAM policy stores are available; otherwise returns nil (disables the policy
// admin endpoints).
func iamPolicyAdminService(state *bootState) admin.IAMPolicyService {
	if state.metaRaft == nil || state.iamPolicyStores == nil {
		return nil
	}
	return NewIAMPolicyAdminAdapter(state.iamPolicyStores, state.metaRaft.Propose)
}

// iamGroupAdminService returns a wired admin.IAMGroupService if MetaRaft is
// available; otherwise returns nil (disables group admin endpoints).
func iamGroupAdminService(state *bootState) admin.IAMGroupService {
	if state.metaRaft == nil {
		return nil
	}
	return &iamGroupAdminAdapter{propose: state.metaRaft.Propose}
}

func ensureProtocolCredentialStore(state *bootState) *protocred.Store {
	if state == nil {
		return nil
	}
	if state.protocolCredentialStore == nil {
		state.protocolCredentialStore = protocred.NewStore()
	}
	if state.metaRaft != nil {
		state.metaRaft.FSM().SetProtocolCredentialStore(state.protocolCredentialStore)
	}
	return state.protocolCredentialStore
}

// lifecycleCascadeEnabled reports whether the bucket-delete cascade may propose
// a lifecycle-delete. It must match the condition under which the lifecycle
// store is wired into the MetaFSM (boot_phases_srvopts.go:261-274,
// cfg.LifecycleInterval > 0): when the store is unwired, applyBucketLifecycleDelete
// returns an error (meta_fsm_exports.go:53) that MetaRaft.Propose surfaces, which
// would break every bucket delete. When unwired no lifecycle config can exist
// (PutBucketLifecycle shares the same nil-store guard), so skipping is correct.
func lifecycleCascadeEnabled(metaRaftPresent bool, lifecycleInterval time.Duration) bool {
	return metaRaftPresent && lifecycleInterval > 0
}

// lifecycleDeleteProposer returns a meta-Raft lifecycle-delete proposer for the
// bucket-delete cascade, or nil (untyped) when lifecycle is not wired — so the
// cascade no-ops rather than proposing a delete that would fail apply.
func lifecycleDeleteProposer(state *bootState) admin.LifecycleDeleteProposer {
	if !lifecycleCascadeEnabled(state.metaRaft != nil, state.cfg.LifecycleInterval) {
		return nil
	}
	return &cluster.LifecycleProposer{Propose: state.metaRaft.Propose}
}

type lifecycleGenReaderAdapter struct{ store *lifecycle.Store }

func (a lifecycleGenReaderAdapter) GetLifecycleGen(_ context.Context, bucket string) (uint64, error) {
	return a.store.GetGen(bucket)
}

// lifecycleGenReader returns a reader for the bucket-delete cascade's
// capture-before-delete, or nil when lifecycle is not wired (cascade observes
// generation 0). Gated identically to lifecycleDeleteProposer.
func lifecycleGenReader(state *bootState) admin.LifecycleGenReader {
	if !lifecycleCascadeEnabled(state.metaRaft != nil, state.cfg.LifecycleInterval) || state.lifecycleStore == nil {
		return nil
	}
	return lifecycleGenReaderAdapter{store: state.lifecycleStore}
}

// bucketUpstreamDeleteProposer returns the IAM bucket-upstream-delete proposer
// for the bucket-delete cascade, or nil when IAM is not wired. Returning the
// concrete *iam.MetaProposer directly when it is nil would box a typed-nil into
// the interface (non-nil interface, defeating the `!= nil` guard), so guard
// explicitly. (The pre-existing `BucketWithPolicyProp: state.iamProposer` line
// has this same latent typed-nil, but boot_phases_backend.go fails boot when
// IAMStore is absent, so it is unreachable on the serve path today — this guard
// is defensive, not load-bearing.)
func bucketUpstreamDeleteProposer(state *bootState) admin.BucketUpstreamDeleteProposer {
	if state.iamProposer == nil {
		return nil
	}
	return state.iamProposer
}

// bucketWithPolicyProposer returns the IAM create-bucket-with-policy proposer
// for the admin bucket-create attach path, or nil when IAM is not wired.
// Returning the concrete *iam.MetaProposer directly when it is nil would box a
// typed-nil into the interface (non-nil interface, defeating the != nil guard
// at handlers_bucket.go:43), so guard explicitly. Unreachable today
// (boot_phases_backend.go fails boot when IAMStore is absent), so this is
// defensive + for consistency with bucketUpstreamDeleteProposer.
func bucketWithPolicyProposer(state *bootState) admin.BucketWithPolicyProposer {
	if state.iamProposer == nil {
		return nil
	}
	return state.iamProposer
}
