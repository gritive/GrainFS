package server

import (
	"os"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/network/standard"

	"github.com/gritive/GrainFS/internal/nodeconfig"
	"github.com/gritive/GrainFS/internal/storage"
)

func normalizeServerStorage(ss ServerStorage, policyStore *CompiledPolicyStore) (ServerStorage, *CompiledPolicyStore) {
	if policyStore == nil {
		policyStore = NewCompiledPolicyStore()
	}
	backend := ss.Backend
	if backend == nil && ss.Ops != nil {
		backend = ss.Ops.Backend()
	}
	if ss.Ops == nil {
		ss.Ops = storage.NewOperations(backend, storage.WithPolicyStore(policyStore))
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

func (s *Server) startRuntimeWorkers() {
	if s.evStore != nil {
		s.startEventWorker()
	}
}
