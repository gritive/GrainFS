package server

import (
	"context"
	"net"
	"strings"

	"github.com/cloudwego/hertz/pkg/app"

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
func (s *Server) icebergGuarded(action string, h app.HandlerFunc) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		// Anon short-circuit FIRST — before even inspecting the Authorization header.
		// When iam.anon-enabled=true the entire bearer gate is bypassed.
		if s.bearerCfg != nil {
			if anon, ok := s.bearerCfg.GetBool("iam.anon-enabled"); ok && anon {
				h(ctx, c)
				return
			}
		}

		authHeader := string(c.GetHeader("Authorization"))
		if !strings.HasPrefix(authHeader, "Bearer ") {
			// No bearer token — fall through to the existing SigV4 path.
			h(ctx, c)
			return
		}

		claims, ok := s.icebergAuthnCheck(ctx, c, authHeader[len("Bearer "):], action)
		if !ok {
			return // response already written
		}
		if claims != nil {
			ctx = context.WithValue(ctx, icebergClaimsKey, claims)
		}
		h(ctx, c)
	}
}

// icebergAuthnCheck validates the bearer token and policy.
// Returns (*Claims, true) on success, (nil, false) on failure (response written).
// Anon short-circuit is handled by the caller (icebergGuarded) before this is invoked.
func (s *Server) icebergAuthnCheck(ctx context.Context, c *app.RequestContext, token, action string) (*iamjwt.Claims, bool) {
	// No JWT keys configured → bearer path unavailable.
	if s.jwtKeys == nil {
		writeIcebergError(c, 401, "NotAuthorizedException", "bearer authentication not configured")
		c.Abort()
		return nil, false
	}

	claims, err := s.jwtKeys.Verify(token)
	if err != nil {
		writeIcebergError(c, 401, "unauthorized", "invalid or expired bearer token: "+err.Error())
		c.Abort()
		return nil, false
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
		return nil, false
	}

	// Policy gate (F#5).
	if s.policyAuthorizer != nil {
		ip := icebergClientIP(c)
		result := s.policyAuthorizer.Authorize(ctx, claims.Sub, claims.Warehouse, policy.RequestContext{
			Action:   action,
			Resource: "arn:aws:s3:::" + claims.Warehouse,
			SourceIP: ip,
		})
		if result.Decision != policy.DecisionAllow {
			writeIcebergError(c, 403, "ForbiddenException", "policy denied: "+result.Reason)
			c.Abort()
			return nil, false
		}
	}

	return claims, true
}

// icebergClientIP extracts the client IP from a Hertz RequestContext.
func icebergClientIP(c *app.RequestContext) string {
	if fwd := string(c.GetHeader("X-Forwarded-For")); fwd != "" {
		if idx := strings.IndexByte(fwd, ','); idx >= 0 {
			return strings.TrimSpace(fwd[:idx])
		}
		return strings.TrimSpace(fwd)
	}
	addr := c.RemoteAddr().String()
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return host
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
