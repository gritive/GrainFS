package serveruntime

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	hzserver "github.com/cloudwego/hertz/pkg/app/server"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/adminapi"
	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/dashboard"
	"github.com/gritive/GrainFS/internal/nodeconfig"
	"github.com/gritive/GrainFS/internal/server"
	"github.com/gritive/GrainFS/internal/server/admin"
	"github.com/gritive/GrainFS/internal/storage"
)

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

	srv := server.New(cfg.Addr, state.backend, state.srvOpts...)
	state.srv = srv

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
	state.adminDeps = &admin.Deps{
		Manager:    srv.VolumeManager(),
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
		VolumePlacement:      NewVolumePlacementAdapter(state.metaRaft),
		IAM:                  state.iamAdminAPI,
		IcebergConfig:        newIcebergConfigAdapter(state.cfg.IAMStore),
		IAMPolicy:            iamPolicyAdminService(state),
		IAMGroup:             iamGroupAdminService(state),
		IAMMountSA:           iamMountSAAdminService(state),
		BucketWithPolicyProp: state.iamProposer,
		ConfigProposer:       state.metaRaft,
		ConfigStore:          state.cfgStore,
		Buckets:              storage.NewOperations(state.backend),
		NfsExports:           &admin.NfsExportServiceAdapter{Svc: state.nfsExportSvc},
		Protocols:            storageProtocolStatusFromConfig(cfg),
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
	dataHertz := srv.HertzEngine()
	dataHertz.Use(server.DashboardTokenMiddleware(tokenStore))
	admin.RegisterUI(dataHertz, state.adminDeps)

	// Open the admin Unix socket. Operator commands (`grainfs volume *`, `grainfs
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
				RegisterClusterConfigRoutes(h, state.metaRaft.FSM(), proposer, state.cfg.Encryptor)

				// KEK envelope admin endpoints (Task 11). Routes register
				// unconditionally so operators always see a well-formed 503
				// "kek admin disabled" when the leader/gate aren't wired,
				// instead of a generic 404. state.kekRotationLeader is nil
				// until the production wiring lands (Phase B Task 11 step 2),
				// at which point the handler proxies through to the leader.
				RegisterEncryptionKEKRoutes(h, state.kekRotationLeader, state.capabilityGate, state.metaRaft.FSM())

				joinH := &JoinHandler{
					dataDir:     cfg.DataDir,
					raftAddr:    state.raftAddr,
					cancel:      state.cancel,
					nodes:       state.metaRaft,
					dataChecker: state.metaRaft.FSM(),
				}
				h.POST("/v1/cluster/join", joinH.Handle)
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

// iamMountSAAdminService returns a wired admin.IAMMountSAService if MetaRaft
// and mountSAStore are available; otherwise returns nil (disables mount-SA
// admin endpoints).
func iamMountSAAdminService(state *bootState) admin.IAMMountSAService {
	if state.metaRaft == nil || state.mountSAStore == nil {
		return nil
	}
	return &iamMountSAAdminAdapter{
		store:   state.mountSAStore,
		propose: state.metaRaft.Propose,
	}
}

func storageProtocolStatusFromConfig(cfg Config) adminapi.StorageProtocolStatusResp {
	return adminapi.StorageProtocolStatusResp{
		NFS4: adminapi.ProtocolEndpointStatus{
			Enabled: cfg.NFS4Port > 0,
			Port:    cfg.NFS4Port,
		},
		NBD: adminapi.ProtocolEndpointStatus{
			Enabled: cfg.NBDPort > 0,
			Port:    cfg.NBDPort,
		},
		P9: adminapi.ProtocolEndpointStatus{
			Enabled: cfg.P9Port > 0,
			Bind:    cfg.P9Bind,
			Port:    cfg.P9Port,
		},
	}
}
