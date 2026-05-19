package server

import (
	"bytes"
	"context"
	"net/http"

	"github.com/cloudwego/hertz/pkg/app"

	iamjwt "github.com/gritive/GrainFS/internal/iam/jwt"
	"github.com/gritive/GrainFS/internal/iam/oauth"
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
