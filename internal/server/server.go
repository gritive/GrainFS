package server

import (
	"context"
	"sync"

	"github.com/cloudwego/hertz/pkg/app/server"

	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/iam/policy"
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
		backend:                     backend,
		ops:                         ss.Ops,
		hub:                         NewHub(),
		policyStore:                 policyStore,
		readAfterWriteRetryTimeout:  defaultReadAfterWriteRetryTimeout,
		readAfterWriteRetryInterval: defaultReadAfterWriteRetryInterval,
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
// dependencies. Call after Options have populated iamStore, iamAudit,
// policyStore, and policyAuthorizer so the authorizer captures their final values.
func (s *Server) buildAuthorizer() {
	var iamStore s3auth.IAMStore
	if s.iamStore != nil {
		iamStore = s.iamStore
	}

	// iamCheck: when a policy authorizer is wired, Layer 1 evaluates
	// policy.Evaluate for the (saID, bucket, action) triple. Without one
	// (test fixtures, legacy mode), deny-by-default is preserved — the
	// bucket policy layer (Layer 2) still runs unconditionally.
	//
	// T51' §6: the closure threads policy decision metadata (matched policy
	// id, matched sid, reason, condition context, anon-allow flag) into the
	// authorizer's audit emitter via the second return value (AuthzDetail).
	var iamCheck s3auth.IAMChecker
	if s.policyAuthorizer != nil {
		pa := s.policyAuthorizer
		iamCheck = func(saID, bucket, key string, action s3auth.S3Action) (bool, s3auth.AuthzDetail) {
			resource := "arn:aws:s3:::" + bucket
			if key != "" {
				resource += "/" + key
			}
			// Note: SourceIP/Prefix are not threaded through IAMChecker today
			// — extending the closure signature is a wider refactor than this
			// fix warrants. ConditionContext picks up aws:Action +
			// aws:Resource (always present), giving operators correlate-by-
			// request facts on every audit row. SourceIP enrichment is a
			// follow-up. T51' B2 review.
			result := pa.Authorize(context.Background(), saID, bucket, policy.RequestContext{
				Action:   action.PolicyActionString(),
				Resource: resource,
			})
			detail := s3auth.AuthzDetail{
				MatchedPolicyID:  result.MatchedPolicy,
				MatchedSID:       result.MatchedSid,
				Reason:           result.Reason,
				ConditionContext: result.ConditionContext,
				// AnonAllow is set when an anonymous request is permitted by
				// either iam.anon-enabled or the default bucket's implicit
				// anon policy (D#2). The authorizer surfaces both via
				// result.Reason; pattern-match here to keep the policy layer
				// agnostic of audit vocabulary.
				AnonAllow: saID == "" && result.Decision == policy.DecisionAllow &&
					(result.Reason == s3auth.ReasonAnonEnabled ||
						result.Reason == s3auth.ReasonDefaultBucketImplicitAnon),
			}
			return result.Decision == policy.DecisionAllow, detail
		}
	} else {
		iamCheck = func(_, _, _ string, _ s3auth.S3Action) (bool, s3auth.AuthzDetail) {
			return false, s3auth.AuthzDetail{}
		}
	}

	s.authz = s3auth.NewRequestAuthorizer(
		iamStore,
		iamCheck,
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

// ReloadTLS re-reads the TLS cert/key from disk and atomically swaps the
// active TLS posture on the HotTLSListener. Wired to SIGHUP in cmd/grainfs/
// serve.go so operators can rotate certs without restart. No-op when the
// listener is absent (e.g. construction-only test fixtures). §5 T43.
func (s *Server) ReloadTLS() error {
	if s.tlsListener == nil {
		return nil
	}
	return s.tlsListener.Reload()
}

// TLSActive reports the current TLS posture of the data-plane listener.
// Used by §5 T44 (TLS posture gate) and operational tooling. §5 T43.
func (s *Server) TLSActive() bool {
	if s.tlsListener == nil {
		return false
	}
	return s.tlsListener.IsTLS()
}

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

// policyDecisionAllow is an alias for the policy Allow decision used in
// buildAuthorizer and iceberg cred-forwarding gate to avoid importing
// policy.DecisionAllow at multiple call sites.
const policyDecisionAllow = policy.DecisionAllow

// policyIcebergConfigContext returns the policy.RequestContext for an
// iceberg:GetCatalogConfig check against a warehouse bucket. Used by
// icebergS3CredOverrides to gate credential forwarding.
func policyIcebergConfigContext(bucket string) policy.RequestContext {
	return policy.RequestContext{
		Action:   "iceberg:GetCatalogConfig",
		Resource: "arn:aws:s3:::" + bucket,
	}
}
