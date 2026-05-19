package server

import (
	"context"
	"sync"

	"github.com/cloudwego/hertz/pkg/app/server"

	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/s3auth"
	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/volume"
)

func NewServerStorage(backend storage.Backend, policyStore *CompiledPolicyStore) ServerStorage {
	return ServerStorage{
		Ops:           storage.NewOperations(backend, storage.WithPolicyStore(policyStore)),
		Backend:       backend,
		VolumeBackend: backend,
		Snapshotable:  storageSnapshotable(backend),
		DBProvider:    storageDBProvider(backend),
	}
}

func storageSnapshotable(backend storage.Backend) storage.Snapshotable {
	if snap, ok := backend.(storage.Snapshotable); ok {
		return snap
	}
	return nil
}

func storageDBProvider(backend storage.Backend) storage.DBProvider {
	if dbp, ok := unwrapBackend(backend).(storage.DBProvider); ok {
		return dbp
	}
	return nil
}

// broadcastLoggerOnce guards the global zerolog.Logger setup so it is wired
// to the SSE hub exactly once, even when multiple Server instances are created.
var broadcastLoggerOnce sync.Once

// New creates a new S3 API server.
func New(addr string, backend storage.Backend, opts ...Option) *Server {
	policyStore := NewCompiledPolicyStore()
	return NewWithServerStorage(addr, NewServerStorage(backend, policyStore), policyStore, opts...)
}

func NewWithServerStorage(addr string, ss ServerStorage, policyStore *CompiledPolicyStore, opts ...Option) *Server {
	ss, policyStore = normalizeServerStorage(ss, policyStore)
	backend := ss.Backend
	s := &Server{
		backend:     backend,
		ops:         ss.Ops,
		hub:         NewHub(),
		policyStore: policyStore,
	}
	for _, opt := range opts {
		opt(s)
	}
	s.buildAuthorizer()
	s.buildMutationBroker()
	if s.mutationGate == nil {
		s.mutationGate = NewMutationGate(nil)
	}

	s.wireAlertState()
	s.wireBroadcastLogger()
	s.initSnapshotManager(ss)

	h := s.newHertzEngine(addr)
	s.ensureRuntimeDefaults(ss)
	s.applyIcebergDiagEnv()
	s.registerRoutes(h)
	s.hertz = h
	s.initMetrics()
	s.startRuntimeWorkers()
	return s
}

// buildAuthorizer wires the request authorizer using the server's current
// dependencies. Call after Options have populated iamStore, iamAudit, and
// policyStore so the authorizer captures their final values.
func (s *Server) buildAuthorizer() {
	var iamStore s3auth.IAMStore
	if s.iamStore != nil {
		iamStore = s.iamStore
	}
	s.authz = s3auth.NewRequestAuthorizer(
		iamStore,
		// iamCheck: legacy Role/Grant path removed in §2. Returning false here
		// causes Layer 1 to deny when auth is enabled and no bucket policy allows.
		// This is intentional — new callers use policy.Evaluate via Authorizer.
		func(_ string, _ string, _ s3auth.S3Action) bool { return false },
		s.policyStore,
		s.iamAudit,
		iam.PrincipalFromContext,
	)
}

// buildMutationBroker wires the mutation broker with the two built-in
// observers (sync metrics + async event emit). Called from NewServer
// after option application so observer construction sees the final
// emit closure.
func (s *Server) buildMutationBroker() {
	s.mutations = NewMutationBroker(
		newMetricsObserver(),
		newEventObserver(s.emitEvent),
	)
}

// isDegraded reports whether the server is currently in EC degraded mode.
func (s *Server) isDegraded() bool { return s.degradedFlag.Load() }

// Run starts the server (blocking). Uses Engine.Run() instead of Spin()
// so that signal handling is owned by the caller (serve.go), not Hertz.
func (s *Server) Run() error {
	return s.hertz.Run()
}

// HertzEngine exposes the underlying Hertz instance so callers (serve.go) can
// install additional middleware and routes (e.g. /ui/api/* admin endpoints,
// dashboard token auth) before Run is called. Must be invoked before Run.
func (s *Server) HertzEngine() *server.Hertz { return s.hertz }

// VolumeManager exposes the volume manager so callers can construct admin.Deps
// without round-tripping through New options.
func (s *Server) VolumeManager() *volume.Manager { return s.volMgr }

// Operations exposes the storage operations facade so external workflows
// (e.g. serveruntime startup recovery) can invoke decorator-aware capability
// helpers like SweepOrphanMultiparts without reaching into Server internals.
func (s *Server) Operations() *storage.Operations { return s.ops }

// Shutdown gracefully shuts down the server, draining in-flight requests.
func (s *Server) Shutdown(ctx context.Context) error {
	err := s.hertz.Shutdown(ctx)
	s.stopEventWorker()
	if closer, ok := s.auditSearcher.(interface{ Close() error }); ok {
		_ = closer.Close()
	}
	if s.alerts != nil {
		s.alerts.Close()
	}
	return err
}

// HealEmitter returns a scrubber.Emitter that fans HealEvents out to the SSE
// hub, the eventstore, and Prometheus. Callers (the serve command) wire it
// into scrubber.New via scrubber.WithEmitter.
func (s *Server) HealEmitter() scrubber.Emitter {
	return newHealEmitter(s.hub, s.emitEvent)
}
