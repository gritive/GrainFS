package server

import (
	"context"
	"time"

	"github.com/cloudwego/hertz/pkg/app"

	"github.com/gritive/GrainFS/internal/audit"
	iamjwt "github.com/gritive/GrainFS/internal/iam/jwt"
	"github.com/gritive/GrainFS/internal/iam/policy"
	"github.com/gritive/GrainFS/internal/s3auth"
)

// icebergClaimsKey is the context key under which *iamjwt.Claims are stored
// after successful bearer authentication.
type icebergClaimsKeyType struct{}

var icebergClaimsKey = icebergClaimsKeyType{}

// icebergGuarded wraps a Hertz handler with bearer-JWT authentication and
// policy gating.  It intercepts only requests with an "Authorization: Bearer …"
// header; all other requests pass through untouched (the existing SigV4
// authMiddleware handles them).
//
// When jwtKeys is nil the wrapper is a transparent no-op (JWT not configured).
// Emits one audit.s3 row per bearer-gated request (allow, deny, anon_allow).
func (s *Server) icebergGuarded(action string, h app.HandlerFunc) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		start := time.Now()

		// Anon short-circuit FIRST — before even inspecting the Authorization header.
		// When iam.anon-enabled=true the entire bearer gate is bypassed.
		if s.bearerCfg != nil {
			if anon, ok := s.bearerCfg.GetBool("iam.anon-enabled"); ok && anon {
				h(ctx, c)
				s.emitIcebergAuditAnonAllow(ctx, c, action, start)
				return
			}
		}

		authHeader := string(c.GetHeader("Authorization"))
		if !hasBearerPrefix(authHeader) {
			// No bearer token — fall through to the existing SigV4 path.
			// Audit for SigV4-authenticated Iceberg requests is outside F29 scope;
			// the S3 audit envelope middleware does not fire on /iceberg/* routes
			// (they have no {bucket} path param), so this is a known gap.
			h(ctx, c)
			return
		}

		claims, evalResult, authzLatencyUS, ok := s.icebergAuthnCheck(ctx, c, trimBearerPrefix(authHeader), action, start)
		if !ok {
			return // response already written; deny audit emitted inside icebergAuthnCheck
		}
		if claims != nil {
			ctx = context.WithValue(ctx, icebergClaimsKey, claims)
		}
		h(ctx, c)
		s.emitIcebergAuditAllow(ctx, c, action, claims, evalResult, authzLatencyUS, start)
	}
}

// icebergAuthnCheck validates the bearer token and policy.
// Returns (*Claims, *EvalResult, authzLatencyUS, true) on success,
// (nil, nil, 0, false) on failure (response written and deny audit emitted).
// Anon short-circuit is handled by the caller (icebergGuarded) before this is invoked.
//
// authzLatencyUS is the microsecond duration of policyAuthorizer.Authorize() only,
// matching the S3 audit envelope semantics for authz_latency_us. It is zero on all
// pre-policy deny paths (bad token, warehouse mismatch) because the policy layer
// was never reached. It is also zero when policyAuthorizer is nil.
//
// start is the request start time, threaded in so deny rows share the same
// timestamp baseline as the allow row that icebergGuarded would emit.
func (s *Server) icebergAuthnCheck(ctx context.Context, c *app.RequestContext, token, action string, start time.Time) (*iamjwt.Claims, *policy.EvalResult, int32, bool) {
	// No JWT keys configured → bearer path unavailable.
	if s.jwtKeys == nil {
		writeIcebergError(c, 401, "NotAuthorizedException", "bearer authentication not configured")
		c.Abort()
		s.emitIcebergAuditDeny(ctx, c, action, "", "", 401, "bearer_not_configured", nil, 0, start)
		return nil, nil, 0, false
	}

	claims, err := s.jwtKeys.Verify(token)
	if err != nil {
		writeIcebergError(c, 401, "unauthorized", "invalid or expired bearer token: "+err.Error())
		c.Abort()
		s.emitIcebergAuditDeny(ctx, c, action, "", "", 401, "invalid_token", nil, 0, start)
		return nil, nil, 0, false
	}

	// Defense in depth (F23): a verified bearer token must carry a non-empty
	// warehouse claim. An empty claim indicates a malformed or tampered token;
	// do not fall through to the SigV4 default-warehouse path.
	if claims.Warehouse == "" {
		writeIcebergError(c, 401, "unauthorized", "bearer token has empty warehouse claim")
		c.Abort()
		s.emitIcebergAuditDeny(ctx, c, action, claims.Sub, "", 401, "empty_warehouse_claim", nil, 0, start)
		return nil, nil, 0, false
	}

	// Warehouse claim cross-check (F#4).
	reqWarehouse := string(c.QueryArgs().Peek("warehouse"))
	if reqWarehouse == "" {
		// Try path param as fallback (routes do not have :warehouse except
		// DELETE /v1/warehouses/:warehouse).
		reqWarehouse = c.Param("warehouse")
	}
	if reqWarehouse != "" && reqWarehouse != claims.Warehouse {
		writeIcebergError(c, 403, "ForbiddenException", "warehouse claim mismatch (F#4)")
		c.Abort()
		s.emitIcebergAuditDeny(ctx, c, action, claims.Sub, claims.Warehouse, 403, "warehouse_mismatch", nil, 0, start)
		return nil, nil, 0, false
	}

	// Policy gate (F#5). §5 T45: SourceIP uses ProxyTrust-validated authoritative
	// client IP rather than naive X-Forwarded-For first-hop — that prevents
	// spoofing by direct clients and rejects untrusted-proxy forwarding chains.
	if s.policyAuthorizer != nil {
		if claims.Sub == audit.SystemSA && claims.Warehouse == audit.Warehouse && auditInternalIcebergReadAction(action) {
			return claims, nil, 0, true
		}
		authzStart := time.Now()
		result := s.policyAuthorizer.Authorize(ctx, claims.Sub, claims.Warehouse, policy.RequestContext{
			Action:   action,
			Resource: "arn:aws:s3:::" + claims.Warehouse,
			SourceIP: s.authoritativeClientIP(c),
		})
		authzLatencyUS := int32(time.Since(authzStart).Microseconds())
		if result.Decision != policy.DecisionAllow {
			writeIcebergError(c, 403, "ForbiddenException", "policy denied: "+result.Reason)
			c.Abort()
			s.emitIcebergAuditDeny(ctx, c, action, claims.Sub, claims.Warehouse, 403, result.Reason, &result, authzLatencyUS, start)
			return nil, nil, 0, false
		}
		return claims, &result, authzLatencyUS, true
	}

	return claims, nil, 0, true
}

// IcebergClaimsFromContext retrieves the *iamjwt.Claims stored by icebergGuarded.
// Returns nil when no bearer authentication occurred (e.g., SigV4 path).
func IcebergClaimsFromContext(ctx context.Context) *iamjwt.Claims {
	v, _ := ctx.Value(icebergClaimsKey).(*iamjwt.Claims)
	return v
}

// catalogWarehouse returns the warehouse to use for catalog operations.
// Priority:
//  1. claims.Warehouse from the verified JWT (bearer path)
//  2. store.Warehouse() as fallback when no claims present (SigV4 anon path)
//
// Bearer requests always use the JWT-bound warehouse, even if the catalog
// has a different default.
func catalogWarehouse(ctx context.Context, store warehouseProvider) string {
	if claims := IcebergClaimsFromContext(ctx); claims != nil && claims.Warehouse != "" {
		return claims.Warehouse
	}
	return store.Warehouse()
}

// warehouseProvider is satisfied by Store and MetaCatalog which retain a
// Warehouse() accessor even though it is no longer part of the Catalog
// interface. Handlers use it for the SigV4 fallback warehouse.
type warehouseProvider interface {
	Warehouse() string
}

// anonConfigReader is a minimal ConfigReader used in tests to simulate iam.anon-enabled.
type anonConfigReader map[string]bool

func (a anonConfigReader) GetBool(key string) (bool, bool) {
	v, ok := a[key]
	return v, ok
}

// staticConfigReader satisfies s3auth.ConfigReader for test use.
var _ s3auth.ConfigReader = anonConfigReader{}
