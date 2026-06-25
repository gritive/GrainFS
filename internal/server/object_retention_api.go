package server

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

// writeObjectLockNotImplemented rejects every Object Lock / retention / legal-hold
// operation with 501 NotImplemented, matching the SSE-C/KMS fail-closed convention
// (sse_headers.go unsupportedSSEHeader, lifecycle_api.go). Object Lock is not
// implemented; returning 200 would be a silent compliance/WORM trust violation,
// and the missing-route variants additionally risk silent object corruption.
func writeObjectLockNotImplemented(c *app.RequestContext) {
	writeXMLError(c, consts.StatusNotImplemented, "NotImplemented", "Object Lock is not supported")
}

// hasObjectLockHeaders reports whether a PutObject request carries any Object Lock
// header. aws-sdk-go-v2 emits these only when the caller sets ObjectLockMode /
// ObjectLockRetainUntilDate / ObjectLockLegalHoldStatus.
func hasObjectLockHeaders(c *app.RequestContext) bool {
	return hasAnyHeader(c,
		"x-amz-object-lock-mode",
		"x-amz-object-lock-retain-until-date",
		"x-amz-object-lock-legal-hold",
	)
}

func (s *Server) getBucketObjectLockConfiguration(ctx context.Context, c *app.RequestContext, bucket string) {
	_ = ctx
	_ = bucket
	writeObjectLockNotImplemented(c)
}

func (s *Server) putObjectRetention(ctx context.Context, c *app.RequestContext) {
	_ = ctx
	writeObjectLockNotImplemented(c)
}

func (s *Server) getObjectRetention(ctx context.Context, c *app.RequestContext) {
	_ = ctx
	writeObjectLockNotImplemented(c)
}

func (s *Server) putObjectLegalHold(ctx context.Context, c *app.RequestContext) {
	_ = ctx
	writeObjectLockNotImplemented(c)
}

func (s *Server) getObjectLegalHold(ctx context.Context, c *app.RequestContext) {
	_ = ctx
	writeObjectLockNotImplemented(c)
}
