package iceberg

import (
	"context"
	"time"

	"github.com/cloudwego/hertz/pkg/app"

	"github.com/gritive/GrainFS/internal/audit"
	iamjwt "github.com/gritive/GrainFS/internal/iam/jwt"
	"github.com/gritive/GrainFS/internal/iam/policy"
)

// icebergGuarded wraps a Hertz handler with bearer-JWT authentication and
// policy gating.  It intercepts only requests with an "Authorization: Bearer …"
// header; all other requests pass through untouched (the existing SigV4
// authMiddleware handles them).
//
// When jwtKeys is nil the wrapper is a transparent no-op (JWT not configured).
// Emits one audit.s3 row per bearer-gated request (allow or deny).
func (h *Handler) icebergGuarded(action string, next app.HandlerFunc) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		start := time.Now()

		authHeader := string(c.GetHeader("Authorization"))
		if !hasBearerPrefix(authHeader) {
			// No bearer token — fall through to the existing SigV4 path.
			// Audit for SigV4-authenticated Iceberg requests is outside F29 scope;
			// the S3 audit envelope middleware does not fire on /iceberg/* routes
			// (they have no {bucket} path param), so this is a known gap.
			next(ctx, c)
			return
		}

		claims, evalResult, authzLatencyUS, ok := h.icebergAuthnCheck(ctx, c, trimBearerPrefix(authHeader), action, start)
		if !ok {
			return // response already written; deny audit emitted inside icebergAuthnCheck
		}
		if claims != nil {
			ctx = context.WithValue(ctx, ClaimsKey, claims)
		}
		next(ctx, c)
		h.emitIcebergAuditAllow(ctx, c, action, claims, evalResult, authzLatencyUS, start)
	}
}

// icebergAuthnCheck validates the bearer token and policy.
// Returns (*Claims, *EvalResult, authzLatencyUS, true) on success,
// (nil, nil, 0, false) on failure (response written and deny audit emitted).
// authzLatencyUS is the microsecond duration of policyAuthorizer.Authorize() only,
// matching the S3 audit envelope semantics for authz_latency_us. It is zero on all
// pre-policy deny paths (bad token, warehouse mismatch) because the policy layer
// was never reached. It is also zero when policyAuthorizer is nil.
//
// start is the request start time, threaded in so deny rows share the same
// timestamp baseline as the allow row that icebergGuarded would emit.
func (h *Handler) icebergAuthnCheck(ctx context.Context, c *app.RequestContext, token, action string, start time.Time) (*iamjwt.Claims, *policy.EvalResult, int32, bool) {
	// No JWT keys configured → bearer path unavailable.
	if h.deps.JWTKeys == nil {
		WriteError(c, 401, "NotAuthorizedException", "bearer authentication not configured")
		c.Abort()
		h.emitIcebergAuditDeny(ctx, c, action, "", "", 401, "bearer_not_configured", nil, 0, start)
		return nil, nil, 0, false
	}

	claims, err := h.deps.JWTKeys.Verify(token)
	if err != nil {
		WriteError(c, 401, "unauthorized", "invalid or expired bearer token: "+err.Error())
		c.Abort()
		h.emitIcebergAuditDeny(ctx, c, action, "", "", 401, "invalid_token", nil, 0, start)
		return nil, nil, 0, false
	}

	// Defense in depth (F23): a verified bearer token must carry a non-empty
	// warehouse claim. An empty claim indicates a malformed or tampered token;
	// do not fall through to the SigV4 default-warehouse path.
	if claims.Warehouse == "" {
		WriteError(c, 401, "unauthorized", "bearer token has empty warehouse claim")
		c.Abort()
		h.emitIcebergAuditDeny(ctx, c, action, claims.Sub, "", 401, "empty_warehouse_claim", nil, 0, start)
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
		WriteError(c, 403, "ForbiddenException", "warehouse claim mismatch (F#4)")
		c.Abort()
		h.emitIcebergAuditDeny(ctx, c, action, claims.Sub, claims.Warehouse, 403, "warehouse_mismatch", nil, 0, start)
		return nil, nil, 0, false
	}

	// Policy gate (F#5). §5 T45: SourceIP uses ProxyTrust-validated authoritative
	// client IP rather than naive X-Forwarded-For first-hop — that prevents
	// spoofing by direct clients and rejects untrusted-proxy forwarding chains.
	if h.deps.PolicyAuthorizer != nil {
		if claims.Sub == audit.SystemSA && claims.Warehouse == audit.Warehouse && auditInternalIcebergReadAction(action) {
			return claims, nil, 0, true
		}
		authzStart := time.Now()
		result := h.deps.PolicyAuthorizer.Authorize(ctx, claims.Sub, claims.Warehouse, policy.RequestContext{
			Action:   action,
			Resource: "arn:aws:s3:::" + claims.Warehouse,
			SourceIP: h.deps.ClientIP(c),
		})
		authzLatencyUS := int32(time.Since(authzStart).Microseconds())
		if result.Decision != policy.DecisionAllow {
			WriteError(c, 403, "ForbiddenException", "policy denied: "+result.Reason)
			c.Abort()
			h.emitIcebergAuditDeny(ctx, c, action, claims.Sub, claims.Warehouse, 403, result.Reason, &result, authzLatencyUS, start)
			return nil, nil, 0, false
		}
		return claims, &result, authzLatencyUS, true
	}

	return claims, nil, 0, true
}

// IcebergClaimsFromContext retrieves the *iamjwt.Claims stored by icebergGuarded.
// Returns nil when no bearer authentication occurred (e.g., SigV4 path).
func IcebergClaimsFromContext(ctx context.Context) *iamjwt.Claims {
	v, _ := ctx.Value(ClaimsKey).(*iamjwt.Claims)
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
