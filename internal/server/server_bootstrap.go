package server

import (
	"os"
	"path/filepath"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/cloudwego/hertz/pkg/app/server"

	"github.com/gritive/GrainFS/internal/icebergcatalog"
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
	h := server.Default(
		server.WithHostPorts(addr),
		server.WithMaxRequestBodySize(512*1024*1024), // 512MB max body
		server.WithMaxKeepBodySize(0),
	)
	s.installMiddlewares(h)
	return h
}

func (s *Server) installMiddlewares(h *server.Hertz) {
	h.Use(s.metricsMiddleware())
	if s.verifier != nil {
		h.Use(s.authMiddleware())
	}
	h.Use(s.auditEnvelopeMiddleware())
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
