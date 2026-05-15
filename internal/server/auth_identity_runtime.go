package server

import (
	"context"

	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/iam"
)

func (s *Server) iamConfigured() bool {
	return s.iamStore != nil
}

func (s *Server) resolveAccessKeyPrincipal(ctx context.Context, accessKey string) (context.Context, *authnFailure) {
	if !s.iamConfigured() {
		return ctx, nil
	}
	k, saID, ok := iam.ResolveSA(s.iamStore, accessKey)
	if !ok {
		return ctx, &authnFailure{
			status: consts.StatusUnauthorized,
			code:   "InvalidAccessKeyId",
			reason: "invalid_access_key",
		}
	}
	ctx = iam.WithPrincipal(ctx, saID)
	ctx = iam.WithPrincipalScope(ctx, k.BucketScope)
	return ctx, nil
}

func (s *Server) signedAuditObjectRead(ctx context.Context, bucket, key, method string) bool {
	return s.iamConfigured() && auditObjectReadRequest(bucket, key, method) && AccessKeyFromContext(ctx) != ""
}

func (s *Server) accessKeyScopeEnforced() bool {
	return s.iamConfigured()
}

func (s *Server) postPolicySigningSecret(accessKey string) string {
	return s.verifier.LookupSecret(accessKey)
}
