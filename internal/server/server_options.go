package server

import (
	"time"

	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/s3auth"
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

// WithIAMAudit wires an AuditLogger that emits IAM authz allow/deny decisions.
func WithIAMAudit(audit *iam.AuditLogger) Option {
	return func(s *Server) {
		s.iamAudit = audit
	}
}

// WithIAMProposer wires the IAM proposer used for auto-grants on CreateBucket.
func WithIAMProposer(p iam.Proposer) Option {
	return func(s *Server) {
		s.iamProposer = p
	}
}

// SetIAMProposer installs the IAM proposer after construction.
func (s *Server) SetIAMProposer(p iam.Proposer) {
	s.iamProposer = p
}

func WithMutationGate(gate *MutationGate) Option {
	return func(s *Server) {
		s.mutationGate = gate
	}
}

func WithAlerts(state *AlertsState) Option {
	return func(s *Server) {
		s.alerts = state
	}
}
