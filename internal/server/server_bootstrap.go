package server

import (
	"os"
	"path/filepath"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/network/standard"

	"github.com/gritive/GrainFS/internal/icebergcatalog"
	"github.com/gritive/GrainFS/internal/nodeconfig"
	"github.com/gritive/GrainFS/internal/snapshot"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/volume"
)

// unwrapBackend returns the innermost backend, unwrapping decorators like CachedBackend.
type unwrapper interface {
	Unwrap() storage.Backend
}

func unwrapBackend(b storage.Backend) storage.Backend {
	for {
		u, ok := b.(unwrapper)
		if !ok {
			return b
		}
		b = u.Unwrap()
	}
}

func normalizeServerStorage(ss ServerStorage, policyStore *CompiledPolicyStore) (ServerStorage, *CompiledPolicyStore) {
	if policyStore == nil {
		policyStore = NewCompiledPolicyStore()
	}
	backend := ss.Backend
	if backend == nil && ss.Ops != nil {
		backend = ss.Ops.Backend()
	}
	if backend == nil {
		backend = ss.VolumeBackend
	}
	if ss.Ops == nil {
		ss.Ops = storage.NewOperations(backend, storage.WithPolicyStore(policyStore))
	}
	if ss.VolumeBackend == nil {
		ss.VolumeBackend = backend
	}
	ss.Backend = backend
	return ss, policyStore
}

func (s *Server) newHertzEngine(addr string) *server.Hertz {
	// §5 T43: bind via HotTLSListener so SIGHUP can hot-swap TLS certs.
	// nodeconfig.New is cheap; it just memoizes dataDir. With an empty
	// dataDir (some construction tests) TLS paths become relative and
	// don't exist → Reload falls through to plaintext, preserving the
	// pre-T43 default posture.
	nc := nodeconfig.New(s.dataDir)
	tlsLn := NewHotTLSListener(nc, addr)
	if err := tlsLn.Start(); err != nil {
		// Partial cert/key or bind failure — fail loud at construction.
		log.Fatal().Err(err).Str("addr", addr).Msg("HotTLSListener.Start failed")
	}
	s.tlsListener = tlsLn

	h := server.Default(
		server.WithListener(tlsLn),
		server.WithTransport(standard.NewTransporter),
		// Match the admin-server pattern (internal/server/admin/server.go):
		// when WithListener is set, WithHostPorts must be empty so Hertz
		// doesn't try to bind a second time.
		server.WithHostPorts(""),
		server.WithMaxRequestBodySize(512*1024*1024), // 512MB max body
		server.WithMaxKeepBodySize(0),
		server.WithStreamBody(true),
	)
	s.installMiddlewares(h)
	return h
}

func (s *Server) installMiddlewares(h *server.Hertz) {
	// WithRequestID must run first so every downstream middleware
	// (metrics, auth, audit, request_log) sees the same rid in context.
	h.Use(WithRequestID())
	h.Use(s.metricsMiddleware())
	if s.verifier != nil {
		h.Use(s.authMiddleware())
	}
	h.Use(s.auditEnvelopeMiddleware())
	h.Use(s.s3RequestLogMiddleware())
	h.Use(s.authzMiddleware())
}

func (s *Server) ensureRuntimeDefaults(ss ServerStorage) {
	if s.volMgr == nil {
		s.volMgr = volume.NewManager(ss.VolumeBackend)
	}
	if s.icebergCatalog == nil {
		if dbp := ss.DBProvider; dbp != nil {
			s.icebergCatalog = icebergcatalog.NewStore(dbp.DB(), "s3://grainfs-tables/warehouse")
		}
	}
}

func (s *Server) wireAlertState() {
	if s.alerts == nil {
		return
	}
	s.alerts.AddOnStateChange(func(degraded bool) {
		s.degradedFlag.Store(degraded)
	})
}

func (s *Server) wireBroadcastLogger() {
	if !broadcastLoggerEnabled {
		return
	}
	broadcastLoggerOnce.Do(func() {
		multi := zerolog.MultiLevelWriter(os.Stderr, &broadcastWriter{hub: s.hub})
		log.Logger = zerolog.New(multi).With().Timestamp().Logger()
	})
}

func (s *Server) initSnapshotManager(ss ServerStorage) {
	if s.dataDir == "" {
		return
	}
	if snap := ss.Snapshotable; snap != nil {
		dir := filepath.Join(s.dataDir, "snapshots")
		walDir := filepath.Join(s.dataDir, "wal")
		if mgr, err := snapshot.NewManagerWithEncryptor(dir, snap, walDir, s.snapshotEnc); err == nil {
			s.snapMgr = mgr
		} else {
			log.Warn().Err(err).Msg("snapshot manager init failed, snapshot/PITR endpoints will be unavailable")
		}
	}
}

func (s *Server) startRuntimeWorkers() {
	if s.evStore != nil {
		s.startEventWorker()
	}
}
