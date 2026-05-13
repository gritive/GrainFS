package serveruntime

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	hzserver "github.com/cloudwego/hertz/pkg/app/server"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/dashboard"
	"github.com/gritive/GrainFS/internal/server"
	"github.com/gritive/GrainFS/internal/server/admin"
)

// bootHTTPServerAndAdmin constructs the data-plane server.Server, wires the
// dashboard token middleware + admin UI routes onto its Hertz engine, and
// opens the admin Unix Domain Socket. Spec A5 invariant: admin routes are
// registered BEFORE the data-plane Hertz starts serving.
//
// Inputs:  state.cfg.Addr, state.backend, state.srvOpts, state.iamProposer,
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

	// P5: wire IAM proposer so CreateBucket auto-issues an Admin grant to
	// the creator SA. nil-safe when iamProposer was not built above.
	if state.iamProposer != nil {
		srv.SetIAMProposer(state.iamProposer)
	}

	// --- Admin / dashboard wiring (Volume CLI Phase B) ---
	tokenStore, err := dashboard.Open(filepath.Join(cfg.DataDir, "dashboard.token"))
	if err != nil {
		return fmt.Errorf("dashboard token: %w", err)
	}
	state.tokenStore = tokenStore
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
		VolumePlacement: NewVolumePlacementAdapter(state.metaRaft),
		IAM:             state.iamAdminAPI,
		Buckets:         state.backend,
	}
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
			srv.RegisterClusterAdminUDS(h)
			if state.metaRaft != nil {
				// Production proposer routes through MetaRaft.Propose, which
				// transparently leader-forwards on followers via the existing
				// forward path (see meta_raft.proposeOrForward).
				proposer := &cluster.ClusterConfigProposer{Propose: state.metaRaft.Propose}
				RegisterClusterConfigRoutes(h, state.metaRaft.FSM(), proposer, state.cfg.Encryptor)

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
