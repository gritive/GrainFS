package server

import (
	"time"

	"github.com/gritive/GrainFS/internal/audit"
	"github.com/gritive/GrainFS/internal/s3auth"
)

func WithAuditEmitter(e *audit.Emitter) Option {
	return func(s *Server) {
		s.auditEmitter = e
	}
}

func WithAuditOutbox(outbox *audit.Outbox) Option {
	return func(s *Server) {
		s.auditOutbox = outbox
	}
}

func WithAuditNodeID(nodeID string) Option {
	return func(s *Server) {
		s.auditNodeID = nodeID
	}
}

func WithAuditSearcher(searcher auditSearcher) Option {
	return func(s *Server) {
		s.auditSearcher = searcher
	}
}

func WithAuditInternalCredentials(accessKey, secretKey string) Option {
	return func(s *Server) {
		if accessKey == "" || secretKey == "" {
			return
		}
		s.auditInternalAccessKey = accessKey
		s.auditInternalVerifier = s3auth.NewCachingVerifier(s3auth.NewVerifier([]s3auth.Credentials{{
			AccessKey: accessKey,
			SecretKey: secretKey,
		}}), 128, 5*time.Minute)
	}
}
