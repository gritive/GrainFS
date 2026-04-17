package server

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/s3auth"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/volume"
)

// ClusterInfo provides cluster state for the monitoring dashboard.
type ClusterInfo interface {
	NodeID() string
	State() string // "Leader", "Follower", "Candidate"
	Term() uint64
	LeaderID() string
	Peers() []string
}

// JoinClusterFunc handles the runtime solo-to-cluster transition.
// The serve layer provides this callback when starting in solo mode.
type JoinClusterFunc func(nodeID, raftAddr, peers, clusterKey string) error

// Server handles S3-compatible API requests using Hertz.
type Server struct {
	backend     storage.Backend
	dataDir     string
	verifier    *s3auth.Verifier
	hertz       *server.Hertz
	volMgr      *volume.Manager
	policyStore *PolicyStore
	ipLimiter   *RateLimiter
	userLimiter *RateLimiter
	cluster     ClusterInfo     // nil in solo mode
	joinCluster JoinClusterFunc // nil if not in solo mode or already clustered
}

// Option configures the server.
type Option func(*Server)

// WithAuth enables SigV4 authentication.
func WithAuth(creds []s3auth.Credentials) Option {
	return func(s *Server) {
		s.verifier = s3auth.NewVerifier(creds)
	}
}

// WithClusterInfo sets the cluster info provider for the monitoring dashboard.
func WithClusterInfo(ci ClusterInfo) Option {
	return func(s *Server) {
		s.cluster = ci
	}
}

// WithJoinCluster sets the callback for runtime solo-to-cluster transition.
func WithJoinCluster(fn JoinClusterFunc) Option {
	return func(s *Server) {
		s.joinCluster = fn
	}
}

// WithDataDir sets the data directory used for snapshot storage.
func WithDataDir(dir string) Option {
	return func(s *Server) {
		s.dataDir = dir
	}
}

// New creates a new S3 API server.
func New(addr string, backend storage.Backend, opts ...Option) *Server {
	s := &Server{
		backend:     backend,
		policyStore: NewPolicyStore(),
		ipLimiter:   NewRateLimiter(100, 200, 100000), // 100 req/sec per IP, burst 200, max 100K entries
		userLimiter: NewRateLimiter(50, 100, 100000),  // 50 req/sec per user, burst 100
	}
	for _, opt := range opts {
		opt(s)
	}

	h := server.Default(
		server.WithHostPorts(addr),
		server.WithMaxRequestBodySize(512*1024*1024), // 512MB max body
	)

	h.Use(s.metricsMiddleware())
	h.Use(s.ipRateLimitMiddleware())

	if s.verifier != nil {
		h.Use(s.authMiddleware())
	}

	h.Use(s.userRateLimitMiddleware())
	h.Use(s.authzMiddleware())

	s.volMgr = volume.NewManager(backend)
	s.registerRoutes(h)
	s.hertz = h
	s.initMetrics()
	return s
}

// Run starts the server (blocking). Uses Engine.Run() instead of Spin()
// so that signal handling is owned by the caller (serve.go), not Hertz.
func (s *Server) Run() error {
	return s.hertz.Run()
}

// Shutdown gracefully shuts down the server, draining in-flight requests.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.hertz.Shutdown(ctx)
}

func (s *Server) authMiddleware() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		// Skip auth for /metrics and /ui/ endpoints
		// Admin endpoints require authentication
		path := string(c.URI().Path())
		if path == "/metrics" || strings.HasPrefix(path, "/ui/") {
			c.Next(ctx)
			return
		}

		// Admin endpoints: require authentication OR localhost access
		if strings.HasPrefix(path, "/admin/debug/") {
			// Check if request is from localhost
			remoteAddr := c.RemoteAddr().String()
			isLocalhost := remoteAddr == "127.0.0.1" ||
				strings.HasPrefix(remoteAddr, "[::1]") ||
				strings.HasPrefix(remoteAddr, "localhost")

			if isLocalhost {
				c.Next(ctx)
				return
			}
		}

		r := toHTTPRequest(c)
		// Check both header auth and query-string presigned auth
		accessKey, err := s.verifier.Verify(r)
		if err != nil {
			writeXMLError(c, consts.StatusForbidden, "AccessDenied", err.Error())
			c.Abort()
			return
		}
		// Propagate identity to downstream handlers
		ctx = WithAccessKey(ctx, accessKey)
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

// initMetrics scans existing buckets and objects to set initial gauge values.
func (s *Server) initMetrics() {
	buckets, err := s.backend.ListBuckets()
	if err != nil {
		return
	}
	metrics.BucketsTotal.Set(float64(len(buckets)))

	var totalObjects int
	var totalBytes int64
	for _, b := range buckets {
		objects, err := s.backend.ListObjects(b, "", 1000000)
		if err != nil {
			continue
		}
		totalObjects += len(objects)
		for _, obj := range objects {
			totalBytes += obj.Size
		}
	}
	metrics.ObjectsTotal.Set(float64(totalObjects))
	metrics.StorageBytesTotal.Set(float64(totalBytes))
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

	// Cluster API (available in both solo and cluster mode)
	h.GET("/api/cluster/status", s.clusterStatus)
	h.POST("/api/cluster/join", s.joinClusterHandler)

	// Volume management API
	volumes := h.Group("/volumes")
	volumes.GET("/", s.listVolumes)
	volumes.PUT("/:name", s.createVolume)
	volumes.GET("/:name", s.getVolume)
	volumes.DELETE("/:name", s.deleteVolume)

	// Snapshot management API
	s.registerSnapshotAPI(h)

	// PITR (Point-in-Time Recovery) API
	s.registerPITRAPI(h)

	// Admin API for testing and operations
	s.registerAdminAPI(h)
}
