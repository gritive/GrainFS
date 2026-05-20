package server

import (
	"context"
	"net/http"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/audit"
	"github.com/gritive/GrainFS/internal/iam"
)

func (s *Server) s3RequestLogMiddleware() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		bucket := c.Param("bucket")
		if bucket == "" {
			c.Next(ctx)
			return
		}

		key := getKey(c)
		query := string(c.URI().QueryString())
		op := audit.ClassifyS3Operation(string(c.Method()), key != "", query, toHTTPRequest(c).Header)
		// Single source of truth: WithRequestID runs first in installMiddlewares
		// and always populates the rid in ctx.
		requestID := RequestIDFromContext(ctx)
		start := time.Now()

		c.Next(ctx)

		status := c.Response.StatusCode()
		if status == 0 {
			status = http.StatusOK
		}
		latency := time.Since(start)
		event := log.WithLevel(s3RequestLogLevel(status, latency)).
			Str("event", "s3.request").
			Str("request_id", requestID).
			Str("method", string(c.Method())).
			Str("operation", op.Operation).
			Str("subresource", op.Subresource).
			Str("bucket", bucket).
			Str("key", key).
			Str("query", query).
			Str("sa_id", iam.PrincipalFromContext(ctx)).
			Int("status", status).
			Int64("bytes_in", s3RequestBytesIn(c)).
			Int64("bytes_out", s3ResponseBytesOut(c)).
			Dur("latency", latency)
		if reason, ok := c.Get(auditErrReasonKey); ok {
			if s, ok := reason.(string); ok && s != "" {
				event = event.Str("err_reason", s)
			}
		}
		event.Msg("s3 request")
	}
}

func s3RequestBytesIn(c *app.RequestContext) int64 {
	if n := c.Request.Header.ContentLength(); n >= 0 {
		return int64(n)
	}
	if c.Request.IsBodyStream() {
		return 0
	}
	return int64(len(c.Request.BodyBytes()))
}

func s3ResponseBytesOut(c *app.RequestContext) int64 {
	if bytesOut, ok := c.Get(auditBytesOutKey); ok {
		if n, ok := bytesOut.(int64); ok {
			return n
		}
	}
	if c.Response.IsBodyStream() {
		if n := c.Response.Header.ContentLength(); n >= 0 {
			return int64(n)
		}
		return 0
	}
	return int64(len(c.Response.Body()))
}

func s3RequestLogLevel(status int, latency time.Duration) zerolog.Level {
	if status >= http.StatusInternalServerError || latency >= time.Second {
		return zerolog.WarnLevel
	}
	return zerolog.DebugLevel
}
