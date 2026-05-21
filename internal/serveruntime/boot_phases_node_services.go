package serveruntime

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/nodeconfig"
	"github.com/gritive/GrainFS/internal/s3auth"
	"github.com/gritive/GrainFS/internal/storage"
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
	// independent intervals. Both are always-on (validated in optionsToConfig).
	// Ring reshard is correctness-critical (stale ring placement blocks reads when
	// old nodes are removed); EC reshard is optimization-only (N×→EC, profile upgrade).
	{
		dgMgr := state.dgMgr

		startRingManager := func(managerCtx context.Context, dg *cluster.DataGroup) {
			gb := dg.Backend()
			leader := gb.Node()
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
	if state.auditSearchWarmup != nil {
		go func() {
			if err := warmupAuditSearch(ctx, state.auditSearchWarmup, 15*time.Second, 500*time.Millisecond, 5*time.Second); err != nil {
				log.Warn().Err(err).Msg("audit search warmup failed")
				return
			}
			log.Info().Msg("audit search warmup completed")
		}()
	}
	return nil
}

func warmupAuditSearch(ctx context.Context, warmup func(context.Context) error, totalTimeout, retryInterval, attemptTimeout time.Duration) error {
	if warmup == nil {
		return nil
	}
	warmupCtx, cancel := context.WithTimeout(ctx, totalTimeout)
	defer cancel()

	for {
		attemptCtx, attemptCancel := context.WithTimeout(warmupCtx, attemptTimeout)
		err := warmup(attemptCtx)
		attemptCancel()
		if err == nil {
			return nil
		}

		timer := time.NewTimer(retryInterval)
		select {
		case <-warmupCtx.Done():
			timer.Stop()
			return err
		case <-timer.C:
		}
	}
}

// bootNodeServicesPostureGate enforces the TLS posture check before NFS/9P
// listeners start. Parallel to bootTLSPostureGate (§5 T44) which gates the S3
// HTTP server; this extends the same §5/FU#3 posture contract to mount
// protocols.
//
// When anon is disabled the NFS/9P connection traverses the same network path
// as S3, so the same posture requirements apply: either a TLS cert on disk or
// a trusted-proxy CIDR set. Returns nil when cfgStore is nil (boot ordering
// guarantees it is non-nil when called from bootNodeServices).
func bootNodeServicesPostureGate(state *bootState) error {
	nc := nodeconfig.New(state.cfg.DataDir)
	if err := enforceTLSPosture(state.cfgStore, nc); err != nil {
		return fmt.Errorf("NFS/9P boot: %w", err)
	}
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
	// NFS§B T13: TLS posture gate — refuse to start NFS/9P when auth is
	// required but neither a TLS cert nor a trusted-proxy CIDR is present.
	// Extends §5/FU#3 gate to mount protocols. cfgStore is guaranteed non-nil
	// at this point (bootSrvOptsAndReceipt fail-fasts on nil).
	if err := bootNodeServicesPostureGate(state); err != nil {
		return err
	}

	cfg := state.cfg
	// Post-Phase-18 local-path merge: universal node services (NFS/NFSv4/NBD)
	// are now wired in cluster mode too, not just local.
	// NFS§B T8: wire mount-SA IAM gate into NFS/9P servers when IAM is available.
	var iamCfg *NodeServicesIAMConfig
	if state.mountSAStore != nil && state.iamPolicyStores != nil && state.cfgStore != nil {
		iamCfg = &NodeServicesIAMConfig{
			MountSAStore: state.mountSAStore,
			Authorizer:   s3auth.NewAuthorizer(state.iamPolicyStores.Resolver, state.cfgStore),
			CfgStore:     state.cfgStore,
		}
	}
	nodeSvc := StartNodeServices(ctx, state.backend, state.volMgr, cfg.NFS4Port, cfg.NBDPort, cfg.P9Bind, cfg.P9Port, state.distBackend, iamCfg)
	nodeSvc.SetNFSExports(state.nfsExportSvc)
	if state.adminDeps != nil {
		state.adminDeps.NFSDiag = nodeSvc.NFS4()
		state.adminDeps.Protocols = nodeSvc.ProtocolStatus(cfg)
	}
	state.AddCleanup(func() { nodeSvc.Close() })
	startNfsExportBucketDeleteCleanup(ctx, state.nfsExportSvc, state.distBackend)

	// Cross-protocol cache coherency: an S3 mutation replicated from another
	// cluster node lands here as an FSM apply that fans out via
	// distBackend.registry. Without this registration, NFS metadata caches
	// (fileMeta, parent-dir mtimes) stay stale until the next backend stat
	// re-fetches.
	if nfs := nodeSvc.NFS4(); nfs != nil {
		state.metaRaft.FSM().SetOnNfsExportChange(func() {
			if err := nfs.RefreshExports(context.Background()); err != nil {
				log.Warn().Err(err).Msg("nfs4: refresh exports after registry apply failed")
			}
		})
		state.AddCleanup(func() { state.metaRaft.FSM().SetOnNfsExportChange(nil) })
		state.distBackend.RegisterCacheInvalidator("nfs4", cluster.CacheInvalidatorFunc(nfs.Invalidate))
	}
	return nil
}

func startNfsExportBucketDeleteCleanup(ctx context.Context, svc interface {
	PendingBucketDeleteCleanups() ([]string, error)
	DeleteForBucketDelete(context.Context, string, bool) error
	ClearBucketDeleteCleanup(string) error
}, backend storage.Backend) {
	if svc == nil || backend == nil {
		return
	}
	run := func() {
		buckets, err := svc.PendingBucketDeleteCleanups()
		if err != nil {
			log.Warn().Err(err).Msg("nfs export cleanup: list pending bucket deletes failed")
			return
		}
		for _, bucket := range buckets {
			if ctx.Err() != nil {
				return
			}
			err := backend.HeadBucket(ctx, bucket)
			if err == nil {
				continue
			}
			if !errors.Is(err, storage.ErrBucketNotFound) {
				log.Warn().Err(err).Str("bucket", bucket).Msg("nfs export cleanup: head bucket failed")
				continue
			}
			if err := svc.DeleteForBucketDelete(ctx, bucket, true); err != nil {
				log.Warn().Err(err).Str("bucket", bucket).Msg("nfs export cleanup: cascade delete failed")
				continue
			}
			if err := svc.ClearBucketDeleteCleanup(bucket); err != nil {
				log.Warn().Err(err).Str("bucket", bucket).Msg("nfs export cleanup: clear marker failed")
			}
		}
	}
	run()
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				run()
			case <-ctx.Done():
				return
			}
		}
	}()
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
