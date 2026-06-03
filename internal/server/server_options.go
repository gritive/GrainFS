package server

import (
	"time"

	"github.com/gritive/GrainFS/internal/iam"
	iamjwt "github.com/gritive/GrainFS/internal/iam/jwt"
	"github.com/gritive/GrainFS/internal/protocred"
	"github.com/gritive/GrainFS/internal/s3auth"
	"github.com/gritive/GrainFS/internal/server/alertssvc"
	"github.com/prometheus/client_golang/prometheus"
)

// Option configures the server.
type Option func(*Server)

// WithAuth enables SigV4 authentication with LRU caching.
func WithAuth(creds []s3auth.Credentials) Option {
	return func(s *Server) {
		s.verifier = s3auth.NewCachingVerifier(s3auth.NewVerifier(creds), 4096, 5*time.Minute)
	}
}

// WithVerifier installs a pre-built CachingVerifier as the auth source.
func WithVerifier(v *s3auth.CachingVerifier) Option {
	return func(s *Server) {
		s.verifier = v
	}
}

// WithIAMStore wires the cluster IAM state container so middlewares can
// resolve principals and check grants.
func WithIAMStore(store *iam.Store) Option {
	return func(s *Server) {
		s.iamStore = store
	}
}

func WithProtocolCredentialAuth(store *protocred.Store, envelope protocred.SecretEnvelope) Option {
	return func(s *Server) {
		if store != nil && envelope != nil {
			s.protocolCredAuth = newProtocolCredentialAuth(store, envelope)
		}
	}
}

// WithIAMAudit wires an AuditLogger that emits IAM authz allow/deny decisions.
func WithIAMAudit(audit *iam.AuditLogger) Option {
	return func(s *Server) {
		s.iamAudit = audit
	}
}

// WithPolicyAuthorizer wires the IAM policy authorizer so Layer 1 (iamCheck)
// evaluates policy.Evaluate for authenticated requests instead of deny-by-default.
// Pass a concrete non-nil implementation, or leave the option unset to disable the
// gate (the s.policyAuthorizer != nil checks rely on an untyped-nil field). Do NOT
// pass a typed-nil concrete (e.g. (*s3auth.Authorizer)(nil)): it wraps to a non-nil
// interface and the nil-checks would let Authorize panic.
func WithPolicyAuthorizer(a PolicyAuthorizer) Option {
	return func(s *Server) {
		s.policyAuthorizer = a
	}
}

// WithJWTKeySet wires the JWT signing key set so the OAuth2 token endpoint
// can mint bearer tokens.
func WithJWTKeySet(ks *iamjwt.KeySet) Option {
	return func(s *Server) {
		s.jwtKeys = ks
	}
}

// WithProxyTrust wires the trusted-proxy validator used by
// (*Server).authoritativeClientIP. §5 T45.
func WithProxyTrust(pt *ProxyTrust) Option {
	return func(s *Server) {
		s.proxyTrust = pt
	}
}

func WithMutationGate(gate *MutationGate) Option {
	return func(s *Server) {
		s.mutationGate = gate
	}
}

func WithAlerts(state *alertssvc.State) Option {
	return func(s *Server) {
		s.alerts = state
	}
}

func WithMetricsGatherer(gatherer prometheus.Gatherer) Option {
	return func(s *Server) {
		s.metricsGatherer = gatherer
	}
}
