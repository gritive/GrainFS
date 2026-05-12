package serveruntime

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster"
)

// bootResharderAndDegraded starts the per-group ReshardManager loop, the
// degraded-mode monitor, the leader-aware lifecycle worker, and the data-plane
// HTTP server goroutine.
//
// Inputs:  state.cfg.ReshardInterval/LifecycleInterval/DegradedInterval,
//
//	state.dgMgr, state.distBackend, state.clusterAlerts, state.node,
//	state.lifecycleSvc, state.srv, state.cfg.Addr.
func bootResharderAndDegraded(ctx context.Context, state *bootState) error {
	cfg := state.cfg

	// Ring reshard and EC reshard share a DataGroup manager registry but run on
	// independent intervals. Both are always-on (validated in buildClusterConfig).
	// Ring reshard is correctness-critical (stale ring placement blocks reads when
	// old nodes are removed); EC reshard is optimization-only (N×→EC, profile upgrade).
	{
		dgMgr := state.dgMgr

		startRingManager := func(managerCtx context.Context, dg *cluster.DataGroup) {
			gb := dg.Backend()
			leader := gb.RaftNode()
			if leader == nil {
				log.Warn().Str("group", dg.ID()).Msg("ring-reshard manager skipped: group has no raft node")
				return
			}
			go cluster.NewRingReshardManager(gb, leader, cfg.RingReshardInterval).Start(managerCtx)
		}
		startECManager := func(managerCtx context.Context, dg *cluster.DataGroup) {
			gb := dg.Backend()
			leader := gb.Node()
			if leader == nil {
				log.Warn().Str("group", dg.ID()).Msg("reshard manager skipped: group has no raft node")
				return
			}
			go cluster.NewReshardManager(gb, leader, cfg.ReshardInterval).Start(managerCtx)
		}

		ringManagers := NewReshardManagerRegistry()
		ringManagers.Refresh(ctx, dgMgr.All(), startRingManager)
		log.Info().Dur("interval", cfg.RingReshardInterval).Msg("ring reshard manager started")

		ecManagers := NewReshardManagerRegistry()
		ecManagers.Refresh(ctx, dgMgr.All(), startECManager)
		log.Info().Dur("interval", cfg.ReshardInterval).Msg("EC reshard manager started")

		if cfg.DataGroupRefreshInterval > 0 {
			go func() {
				ticker := time.NewTicker(cfg.DataGroupRefreshInterval)
				defer ticker.Stop()
				for {
					select {
					case <-ctx.Done():
						return
					case <-ticker.C:
						ringManagers.Refresh(ctx, dgMgr.All(), startRingManager)
						ecManagers.Refresh(ctx, dgMgr.All(), startECManager)
					}
				}
			}()
		}
	}

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
	return nil
}

// bootNodeServices starts the universal node services (NFS/NFSv4/NBD) and
// registers the NFS4 cache invalidator on distBackend so cross-protocol cache
// coherency works in cluster mode.
//
// Inputs:  state.backend, state.volMgr, state.cfg.NFS4Port/NBDPort,
//
//	state.distBackend.
//
// Cleanup: nodeSvc.Close registered via state.AddCleanup.
func bootNodeServices(ctx context.Context, state *bootState) error {
	cfg := state.cfg
	// Post-Phase-18 local-path merge: universal node services (NFS/NFSv4/NBD)
	// are now wired in cluster mode too, not just local.
	nodeSvc := StartNodeServices(state.backend, state.volMgr, cfg.NFS4Port, cfg.NBDPort, state.distBackend)
	state.AddCleanup(func() { nodeSvc.Close() })

	// Cross-protocol cache coherency: an S3 mutation replicated from another
	// cluster node lands here as an FSM apply that fans out via
	// distBackend.registry. Without this registration, NFS metadata caches
	// (fileMeta, parent-dir mtimes) stay stale until the next backend stat
	// re-fetches.
	if nfs := nodeSvc.NFS4(); nfs != nil {
		state.distBackend.RegisterCacheInvalidator("nfs4", cluster.CacheInvalidatorFunc(nfs.Invalidate))
	}
	return nil
}

// bootShutdownDrain blocks on ctx.Done(), then runs the ordered drain
// sequence: admin UDS first (spec A5), then data-plane HTTP, then Raft
// leadership transfer + apply-loop stop.
//
// This is NOT registered through AddCleanup because the drain order is the
// inverse of generic cleanup LIFO — admin must stop FIRST, before HTTP, before
// raft. The state.AddCleanup-registered fallbacks (adminSrv.Stop best-effort,
// WAL close, etc.) still run after this returns via the deferred state.Cleanup
// in Run().
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
