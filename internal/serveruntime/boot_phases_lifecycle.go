package serveruntime

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster"
)

// bootDegradedAndServices starts the degraded-mode monitor, the leader-aware
// lifecycle worker, the migration service, and the data-plane HTTP server
// goroutine.
//
// Inputs:  state.cfg.LifecycleInterval/MigrationInterval/DegradedInterval,
//
//	state.distBackend, state.clusterAlerts, state.node,
//	state.lifecycleSvc, state.migrationSvc, state.srv, state.cfg.Addr.
func bootDegradedAndServices(ctx context.Context, state *bootState) error {
	cfg := state.cfg

	// Start the leader-aware worker loop. Only the Raft leader runs the
	// worker; followers skip the scan so we don't waste IO on proposals that
	// would be rejected anyway.
	if state.lifecycleSvc != nil {
		go state.lifecycleSvc.Run(ctx)
		log.Info().Dur("interval", cfg.LifecycleInterval).Msg("cluster lifecycle service started")
	}

	if state.migrationSvc != nil {
		go state.migrationSvc.Run(ctx)
		log.Info().Dur("interval", cfg.MigrationInterval).Msg("migration service started")
	}

	// Start the degraded mode monitor — checks live node count vs EC threshold
	// every 30 s. The first check fires immediately so the server knows its
	// state before serving any requests.
	degradedMon := cluster.NewDegradedMonitor(state.distBackend, state.clusterAlerts.Tracker(), cfg.DegradedInterval).
		WithQuorumCheck(state.node, state.clusterAlerts)
	go degradedMon.Run(ctx)

	addr := cfg.Addr
	srv := state.srv
	go func() {
		if err := srv.Run(); err != nil {
			log.Error().Err(err).Str("addr", addr).
				Msg("http server error — confirm TCP port is free (lsof -i TCP:" + addr + "), or pass --port=0 to pick a free port")
		}
	}()

	// §5 T43: SIGHUP → ReloadTLS. Operators rotate certs by writing fresh
	// PEM files to TLSCertPath/TLSKeyPath and `kill -HUP <grainfs-pid>`.
	// Lives in its own goroutine on a dedicated channel so it does NOT
	// race with the SIGINT/SIGTERM signal.NotifyContext path in serve.go.
	// The channel exits when ctx is done; signal.Stop releases the OS hook.
	hupCh := make(chan os.Signal, 1)
	signal.Notify(hupCh, syscall.SIGHUP)
	go func() {
		defer signal.Stop(hupCh)
		for {
			select {
			case <-ctx.Done():
				return
			case <-hupCh:
				if err := srv.ReloadTLS(); err != nil {
					log.Warn().Err(err).Msg("SIGHUP: TLS reload failed; previous posture retained")
				} else {
					log.Info().Bool("tls_active", srv.TLSActive()).Msg("SIGHUP: TLS reloaded")
				}
			}
		}
	}()
	return nil
}

// bootShutdownDrain waits for the context to be cancelled then performs a
// graceful shutdown sequence: admin socket → data-plane → raft leadership
// transfer → stop apply loop.  Called from the tail of Run().
func bootShutdownDrain(ctx context.Context, state *bootState) {
	<-ctx.Done()
	log.Info().Str("component", "server").Msg("graceful shutdown started")

	// 1. Stop admin Unix socket FIRST so operator commands fail fast with
	// "connection refused" instead of landing on a server that's draining.
	// Spec A5: admin-first, data-plane-second.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := state.adminSrv.Stop(shutdownCtx); err != nil {
		log.Warn().Err(err).Msg("admin server shutdown error")
	}

	// 2. Drain in-flight HTTP requests on the data plane.
	if err := state.srv.Shutdown(shutdownCtx); err != nil {
		log.Warn().Err(err).Msg("http server shutdown error")
	}

	// 3. Transfer Raft leadership before stopping
	if err := state.node.TransferLeadership(); err != nil {
		log.Debug().Err(err).Msg("leadership transfer skipped")
	} else {
		log.Info().Str("component", "raft").Msg("leadership transferred")
	}

	// 4. Stop Raft apply loop
	close(state.stopApply)

	log.Info().Str("component", "server").Msg("server stopped")
}
