package server

import (
	"bytes"
	"context"
	"net/http"

	"github.com/cloudwego/hertz/pkg/app"

	"github.com/gritive/GrainFS/internal/audit"
	iamjwt "github.com/gritive/GrainFS/internal/iam/jwt"
	"github.com/gritive/GrainFS/internal/iam/oauth"
	"github.com/gritive/GrainFS/internal/iam/policy"
)

// icebergOAuthHandler wraps the stdlib oauth.Handler for use with Hertz.
// The oauth.Handler parses form bodies via r.ParseForm(); we reconstruct
// a stdlib http.Request with the raw POST body so ParseForm works correctly.
type icebergOAuthHandler struct {
	inner *oauth.Handler
}

func newIcebergOAuthHandler(sa oauth.SAResolver, keys *iamjwt.KeySet, authz oauth.Authorizer) *icebergOAuthHandler {
	return &icebergOAuthHandler{inner: oauth.NewHandler(sa, keys, authz)}
}

type auditInternalOAuthResolver struct {
	base      oauth.SAResolver
	accessKey string
	secretKey string
}

func (r auditInternalOAuthResolver) LookupByAccessKey(ctx context.Context, accessKey string) (string, []byte, error) {
	if r.accessKey != "" && accessKey == r.accessKey {
		return audit.SystemSA, []byte(r.secretKey), nil
	}
	return r.base.LookupByAccessKey(ctx, accessKey)
}

type auditInternalOAuthAuthorizer struct {
	base oauth.Authorizer
}

func (a auditInternalOAuthAuthorizer) Authorize(ctx context.Context, saID, bucket string, ctxReq policy.RequestContext) policy.EvalResult {
	if saID == audit.SystemSA && bucket == audit.Warehouse && auditInternalIcebergReadAction(ctxReq.Action) {
		return policy.EvalResult{Decision: policy.DecisionAllow, Reason: "audit internal iceberg reader"}
	}
	return a.base.Authorize(ctx, saID, bucket, ctxReq)
}

func auditInternalIcebergReadAction(action string) bool {
	switch action {
	case "iceberg:GetCatalogConfig",
		"iceberg:ListNamespaces",
		"iceberg:LoadNamespace",
		"iceberg:HeadNamespace",
		"iceberg:ListTables",
		"iceberg:LoadTable",
		"iceberg:HeadTable":
		return true
	default:
		return false
	}
}

func (h *icebergOAuthHandler) handle(ctx context.Context, c *app.RequestContext) {
	body := c.Request.Body()
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, "/", bytes.NewReader(body))
	if err != nil {
		c.Status(http.StatusInternalServerError)
		return
	}
	// Copy headers so ParseForm and BasicAuth work.
	c.Request.Header.VisitAll(func(k, v []byte) {
		r.Header.Set(string(k), string(v))
	})
	w := newResponseWriter(c)
	h.inner.ServeHTTP(w, r)
}

// oauthHandlerFunc returns a Hertz handler function that delegates to the
// wired icebergOAuthHandler.  Returns a 503 handler when no oauth handler
// is configured (e.g. tests that don't call WithJWTKeySet + WithIAMStore).
func (s *Server) oauthHandlerFunc() app.HandlerFunc {
	if s.oauthHandler != nil {
		return s.oauthHandler.handle
	}
	return func(_ context.Context, c *app.RequestContext) {
		c.JSON(http.StatusServiceUnavailable,
			map[string]string{"error": "temporarily_unavailable"})
	}
}
