package admin

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"
)

// peerCredCtxKey is the unexported context key used to thread the resolved
// peer credentials of an admin UDS caller through Hertz's per-request ctx.
// Callers reach the value via WithPeerCred / PeerCredFromContext below.
type peerCredCtxKey struct{}

var peerCredCtxKeyInstance peerCredCtxKey

// PeerCredValue is the typed shape that admin middleware writes and
// clusteradmin handlers read. Exporting the value type (not the unexported
// peerCred struct) keeps the cross-package contract explicit while keeping
// the listener-side types unexported.
type PeerCredValue struct {
	UID      uint32
	Resolved bool
}

// WithPeerCred returns a new context with the given peer credentials
// attached. Mirrors the pattern in internal/server/auth_context.go.
func WithPeerCred(ctx context.Context, v PeerCredValue) context.Context {
	return context.WithValue(ctx, peerCredCtxKeyInstance, v)
}

// PeerCredFromContext returns the peer credentials previously attached by
// WithPeerCred. The second return value is false when no middleware ran on
// this request (e.g., non-admin Hertz path, or pre-middleware error). On
// supported OSes with the middleware installed, ok will be true and the
// returned value carries Resolved=true|false depending on whether
// SO_PEERCRED/LOCAL_PEERCRED succeeded at Accept time.
func PeerCredFromContext(ctx context.Context) (PeerCredValue, bool) {
	v, ok := ctx.Value(peerCredCtxKeyInstance).(PeerCredValue)
	return v, ok
}

// peerCredMiddleware reads c.GetConn().RemoteAddr(), type-asserts to
// *peerCredAddr (the typed addr produced by peerCredListener at Accept
// time), and attaches the credentials to the ctx that Hertz threads through
// app.HandlerFunc and into admin handler wrappers like wrapZero/wrapBody.
// Consumers read them via PeerCredFromContext.
//
// Fail-open: nil conn, or any RemoteAddr that is not a *peerCredAddr (e.g.,
// in tests using a synthetic listener), passes through without storing a
// value. Forensic enrichment, never authorization.
func peerCredMiddleware() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		conn := c.GetConn()
		if conn == nil {
			c.Next(ctx)
			return
		}
		pca, ok := conn.RemoteAddr().(*peerCredAddr)
		if !ok {
			c.Next(ctx)
			return
		}
		cred := pca.Cred()
		ctx = WithPeerCred(ctx, PeerCredValue(cred))
		c.Next(ctx)
	}
}
