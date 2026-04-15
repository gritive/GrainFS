package server

import (
	"context"
	"strconv"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/s3auth"
	"github.com/gritive/GrainFS/internal/storage"
)

// Server handles S3-compatible API requests using Hertz.
type Server struct {
	backend  storage.Backend
	verifier *s3auth.Verifier
	hertz    *server.Hertz
}

// Option configures the server.
type Option func(*Server)

// WithAuth enables SigV4 authentication.
func WithAuth(creds []s3auth.Credentials) Option {
	return func(s *Server) {
		s.verifier = s3auth.NewVerifier(creds)
	}
}

// New creates a new S3 API server.
func New(addr string, backend storage.Backend, opts ...Option) *Server {
	s := &Server{backend: backend}
	for _, opt := range opts {
		opt(s)
	}

	h := server.Default(
		server.WithHostPorts(addr),
		server.WithMaxRequestBodySize(512*1024*1024), // 512MB max body
	)

	h.Use(s.metricsMiddleware())

	if s.verifier != nil {
		h.Use(s.authMiddleware())
	}

	s.registerRoutes(h)
	s.hertz = h
	return s
}

// Run starts the server (blocking).
func (s *Server) Run() {
	s.hertz.Spin()
}

func (s *Server) authMiddleware() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		// Skip auth for /metrics and /ui/ endpoints
		path := string(c.URI().Path())
		if path == "/metrics" || path == "/ui/" {
			c.Next(ctx)
			return
		}

		r := toHTTPRequest(c)
		// Check both header auth and query-string presigned auth
		if _, err := s.verifier.Verify(r); err != nil {
			writeXMLError(c, consts.StatusForbidden, "AccessDenied", err.Error())
			c.Abort()
			return
		}
		c.Next(ctx)
	}
}

func (s *Server) metricsMiddleware() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		start := time.Now()
		c.Next(ctx)
		duration := time.Since(start).Seconds()
		method := string(c.Method())
		status := strconv.Itoa(c.Response.StatusCode())
		metrics.HTTPRequestsTotal.WithLabelValues(method, status).Inc()
		metrics.HTTPRequestDuration.WithLabelValues(method).Observe(duration)
	}
}

func (s *Server) registerRoutes(h *server.Hertz) {
	// Prometheus metrics endpoint (no auth)
	promHandler := promhttp.Handler()
	h.GET("/metrics", func(_ context.Context, c *app.RequestContext) {
		promHandler.ServeHTTP(newResponseWriter(c), toHTTPRequest(c))
	})

	// Dashboard UI
	h.GET("/ui/", s.serveDashboard)

	// Service-level: list buckets
	h.GET("/", s.listBuckets)

	// Bucket-level
	h.PUT("/:bucket", s.createBucket)
	h.HEAD("/:bucket", s.headBucket)
	h.DELETE("/:bucket", s.deleteBucket)
	h.GET("/:bucket", s.listObjects)

	// Object-level: Hertz uses *path to catch nested keys
	h.PUT("/:bucket/*key", s.handlePut)
	h.GET("/:bucket/*key", s.getObject)
	h.HEAD("/:bucket/*key", s.headObject)
	h.DELETE("/:bucket/*key", s.deleteObject)

	// Multipart: POST /:bucket/*key with ?uploads or ?uploadId=
	h.POST("/:bucket/*key", s.handlePost)
}
