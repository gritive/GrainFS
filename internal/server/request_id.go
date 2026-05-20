// Request-ID middleware (auth-redesign §5 T41).
//
// Every public HTTP response carries X-GrainFS-Request-Id. If the caller
// supplies the header on the incoming request, it is preserved verbatim;
// otherwise the server generates a UUIDv7 (falling back to UUIDv4 if v7
// generation fails). The same value is dual-written to x-amz-request-id so
// S3 clients see the conventional header, and stored in the request context
// so downstream middlewares (auth, audit, request_log, error envelopes)
// share a single source of truth.
package server

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/google/uuid"
)

// RequestIDHeader is the canonical GrainFS request-id response header.
const RequestIDHeader = "X-GrainFS-Request-Id"

// amzRequestIDHeader is the conventional S3 request-id header. Dual-written
// so AWS SDK clients continue to surface the rid in their error metadata.
const amzRequestIDHeader = "x-amz-request-id"

type reqIDKey struct{}

// requestIDHertzKey is the c.Set/c.Get key under which WithRequestID stashes
// the rid for synchronous read by writers that only hold *app.RequestContext
// (e.g. S3 / Iceberg error envelopes). This dual-write is atomic with the
// context.WithValue write inside WithRequestID, so there is only one writer
// and no drift risk.
const requestIDHertzKey = "grainfs.request_id"

// WithRequestID returns a Hertz middleware that ensures every request has a
// rid in context and on the response. Must run first in the middleware chain
// so auth / audit / request_log all observe the same rid.
func WithRequestID() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		rid := string(c.GetHeader(RequestIDHeader))
		if rid == "" {
			id, err := uuid.NewV7()
			if err != nil {
				id = uuid.New() // fallback to v4 — extremely unlikely path
			}
			rid = id.String()
		}
		c.Header(RequestIDHeader, rid)
		c.Header(amzRequestIDHeader, rid)
		c.Set(requestIDHertzKey, rid)
		ctx = context.WithValue(ctx, reqIDKey{}, rid)
		c.Next(ctx)
	}
}

// RequestIDFromContext returns the rid attached by WithRequestID, or empty
// if WithRequestID did not run upstream.
func RequestIDFromContext(ctx context.Context) string {
	v, _ := ctx.Value(reqIDKey{}).(string)
	return v
}

// requestIDFromHertz reads the rid from the Hertz request context's K/V store.
// Returns empty if WithRequestID did not run. Used by error envelope writers
// (S3 XML, Iceberg JSON) that only hold *app.RequestContext and would
// otherwise need ctx plumbed through hundreds of call sites.
func requestIDFromHertz(c *app.RequestContext) string {
	v, ok := c.Get(requestIDHertzKey)
	if !ok {
		return ""
	}
	s, _ := v.(string)
	return s
}
