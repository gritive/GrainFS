package server

import (
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/gritive/GrainFS/internal/cache/blockcache"
	"github.com/gritive/GrainFS/internal/cache/shardcache"
	"github.com/gritive/GrainFS/internal/eventstore"
	"github.com/gritive/GrainFS/internal/lifecycle"
	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/receipt"
	"github.com/gritive/GrainFS/internal/s3auth"
	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/snapshot"
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
	// LivePeers returns the subset of peers that are currently reachable.
	// Used to compute down_nodes in the cluster status endpoint.
	LivePeers() []string
}

// JoinClusterFunc handles the runtime local-to-cluster transition.
// The serve layer provides this callback when starting in no-peers mode.
type JoinClusterFunc func(nodeID, raftAddr, peers, clusterKey string) error

// Server handles S3-compatible API requests using Hertz.
type Server struct {
	backend        storage.Backend
	dataDir        string
	snapMgr        *snapshot.Manager
	scrubber       *scrubber.BackgroundScrubber // nil if not using ECBackend
	verifier       *s3auth.CachingVerifier
	hertz          *server.Hertz
	hub            *Hub
	volMgr         *volume.Manager
	policyStore    *CompiledPolicyStore
	lifecycleStore *lifecycle.Store
	ipLimiter      *RateLimiter
	userLimiter    *RateLimiter
	cluster        ClusterInfo       // nil in no-peers mode
	joinCluster    JoinClusterFunc   // nil if not in no-peers mode or already clustered
	balancer       BalancerInfo      // nil if balancer not enabled
	evStore        *eventstore.Store // nil if event store not configured
	alerts         *AlertsState      // nil if alerts not wired
	receiptAPI     *receipt.API      // nil when heal-receipt API disabled (Phase 16 Slice 2)
	degradedFlag   atomic.Bool       // true when EC degraded mode is active
	blockCache     *blockcache.Cache // nil 또는 비활성. /api/cache/status가 노출.
	shardCache     *shardcache.Cache // nil 또는 비활성. EC shard cache, /api/cache/status에 함께 노출.

	// Bounded event queue + single worker. Decouples request handlers from
	// BadgerDB write latency and prevents unbounded goroutine growth.
	eventCh       chan eventstore.Event
	eventDone     chan struct{}
	eventStopOnce sync.Once
}

// broadcastLoggerOnce guards the global zerolog.Logger setup so it is wired
// to the SSE hub exactly once, even when multiple Server instances are created.
var broadcastLoggerOnce sync.Once

// Option configures the server.
type Option func(*Server)

// WithAuth enables SigV4 authentication with LRU caching.
func WithAuth(creds []s3auth.Credentials) Option {
	return func(s *Server) {
		s.verifier = s3auth.NewCachingVerifier(s3auth.NewVerifier(creds), 4096, 5*time.Minute)
	}
}

// WithBlockCache wires the volume block cache so /api/cache/status can
// expose its hits/misses/resident bytes for the dashboard.
func WithBlockCache(c *blockcache.Cache) Option {
	return func(s *Server) {
		s.blockCache = c
	}
}

// WithShardCache wires the EC shard cache so /api/cache/status can
// expose its hits/misses/resident bytes alongside the block cache.
func WithShardCache(c *shardcache.Cache) Option {
	return func(s *Server) {
		s.shardCache = c
	}
}

// WithClusterInfo sets the cluster info provider for the monitoring dashboard.
func WithClusterInfo(ci ClusterInfo) Option {
	return func(s *Server) {
		s.cluster = ci
	}
}

// WithBalancerInfo sets the balancer status provider for the health endpoint.
func WithBalancerInfo(bi BalancerInfo) Option {
	return func(s *Server) {
		s.balancer = bi
	}
}

// WithJoinCluster sets the callback for runtime local-to-cluster transition.
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

// WithScrubber sets the background scrubber (only used with ECBackend).
func WithScrubber(sc *scrubber.BackgroundScrubber) Option {
	return func(s *Server) {
		s.scrubber = sc
	}
}

// WithLifecycleStore attaches a lifecycle rule store to the server.
func WithLifecycleStore(store *lifecycle.Store) Option {
	return func(s *Server) {
		s.lifecycleStore = store
	}
}

// WithEventStore attaches an event store to the server for audit logging.
func WithEventStore(store *eventstore.Store) Option {
	return func(s *Server) {
		s.evStore = store
	}
}

// WithReceiptAPI wires the Phase 16 Slice 2 heal-receipt read API.
// When set, /api/receipts/:id and /api/receipts?from=&to= become live.
func WithReceiptAPI(api *receipt.API) Option {
	return func(s *Server) {
		s.receiptAPI = api
	}
}

// WithAlerts attaches an AlertsState (Phase 16 Week 4) so the server can
// expose /api/admin/alerts/{status,resend} and let other components push
// fault/healthy reports through s.Alerts().Tracker().
func WithAlerts(state *AlertsState) Option {
	return func(s *Server) {
		s.alerts = state
	}
}

// WithVolumeManager injects a pre-built volume.Manager (e.g. one with dedup enabled).
// When not set, New() creates a plain manager via volume.NewManager(backend).
func WithVolumeManager(mgr *volume.Manager) Option {
	return func(s *Server) {
		s.volMgr = mgr
	}
}

// WithRateLimits overrides the default IP/user rate limits.
// Setting an rps to 0 (or negative) disables that layer entirely — useful for
// benchmarking, dev, or when an upstream proxy already enforces limits.
// Defaults: ip=100 rps/burst 200, user=50 rps/burst 100.
func WithRateLimits(ipRPS float64, ipBurst int, userRPS float64, userBurst int) Option {
	return func(s *Server) {
		s.ipLimiter = NewRateLimiter(ipRPS, ipBurst, 100000)
		s.userLimiter = NewRateLimiter(userRPS, userBurst, 100000)
	}
}

// New creates a new S3 API server.
func New(addr string, backend storage.Backend, opts ...Option) *Server {
	s := &Server{
		backend:     backend,
		hub:         NewHub(),
		policyStore: NewCompiledPolicyStore(),
		ipLimiter:   NewRateLimiter(100, 200, 100000), // 100 req/sec per IP, burst 200, max 100K entries
		userLimiter: NewRateLimiter(50, 100, 100000),  // 50 req/sec per user, burst 100
	}
	for _, opt := range opts {
		opt(s)
	}

	// Wire degradedFlag into the AlertsState tracker so it stays in sync with
	// the tracker's degraded state without needing to poll.
	if s.alerts != nil {
		s.alerts.AddOnStateChange(func(degraded bool) {
			s.degradedFlag.Store(degraded)
		})
	}

	// Route zerolog global logger through broadcastWriter so every log line
	// is also fanned out to SSE dashboard clients.
	// Once-guard prevents double-wrapping when multiple Server instances share a process (e.g. tests).
	broadcastLoggerOnce.Do(func() {
		multi := zerolog.MultiLevelWriter(os.Stderr, &broadcastWriter{hub: s.hub})
		log.Logger = zerolog.New(multi).With().Timestamp().Logger()
	})

	// Initialize snapshot manager once (avoids per-request allocation and concurrent seq collisions).
	if s.dataDir != "" {
		if snap, ok := s.backend.(storage.Snapshotable); ok {
			dir := filepath.Join(s.dataDir, "snapshots")
			walDir := filepath.Join(s.dataDir, "wal")
			if mgr, err := snapshot.NewManager(dir, snap, walDir); err == nil {
				s.snapMgr = mgr
			} else {
				log.Warn().Err(err).Msg("snapshot manager init failed, snapshot/PITR endpoints will be unavailable")
			}
		}
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

	if s.volMgr == nil {
		s.volMgr = volume.NewManager(backend)
	}
	s.registerRoutes(h)
	s.hertz = h
	s.initMetrics()
	if s.evStore != nil {
		s.startEventWorker()
	}
	return s
}

// isDegraded reports whether the server is currently in EC degraded mode.
func (s *Server) isDegraded() bool { return s.degradedFlag.Load() }

// Run starts the server (blocking). Uses Engine.Run() instead of Spin()
// so that signal handling is owned by the caller (serve.go), not Hertz.
func (s *Server) Run() error {
	return s.hertz.Run()
}

// Shutdown gracefully shuts down the server, draining in-flight requests.
func (s *Server) Shutdown(ctx context.Context) error {
	err := s.hertz.Shutdown(ctx)
	s.stopEventWorker()
	if s.alerts != nil {
		s.alerts.Close()
	}
	return err
}

func (s *Server) authMiddleware() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		// Skip auth for monitoring/read-only status endpoints and /ui/
		path := string(c.URI().Path())
		if path == "/metrics" || strings.HasPrefix(path, "/ui/") ||
			path == "/api/events" || path == "/api/events/heal/stream" ||
			path == "/api/eventlog" ||
			path == "/api/cluster/status" || path == "/api/health" ||
			path == "/api/cache/status" {
			c.Next(ctx)
			return
		}

		// All /admin/ endpoints: allow localhost without credentials.
		if strings.HasPrefix(path, "/admin/") {
			if isLocalhostAddr(c.RemoteAddr().String()) {
				c.Next(ctx)
				return
			}
		}

		r := toHTTPRequest(c)

		// Anonymous fast path: unsigned GET/HEAD on plain object data may be allowed
		// by object ACL. Subresource requests (?acl, ?tagging, etc.) and all other
		// methods always require authentication — checked via RawQuery == "".
		method := string(c.Method())
		key := strings.TrimPrefix(c.Param("key"), "/")
		isObjectRead := (method == "GET" || method == "HEAD") && key != "" && r.URL.RawQuery == ""
		if isObjectRead && r.Header.Get("Authorization") == "" {
			ctx = WithAccessKey(ctx, "")
			c.Next(ctx)
			return
		}

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

// isLocalhostAddr reports whether addr (typically from c.RemoteAddr().String(),
// in "host:port" or "[host]:port" form) is a loopback address.
// Covers IPv4 loopback, IPv6 loopback, the IPv4-mapped IPv6 loopback
// ([::ffff:127.0.0.1]:PORT), and the literal "localhost" hostname.
func isLocalhostAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr // no port present
	}
	switch host {
	case "127.0.0.1", "::1", "::ffff:127.0.0.1", "localhost":
		return true
	}
	return false
}

// localhostOnly returns a middleware that rejects non-localhost connections with 403.
func localhostOnly() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		if !isLocalhostAddr(c.RemoteAddr().String()) {
			c.JSON(consts.StatusForbidden, map[string]string{
				"error": "admin endpoints are restricted to localhost",
			})
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

	// Cluster API (available in both local and cluster mode)
	h.GET("/api/cluster/status", s.clusterStatus)
	h.GET("/api/cache/status", s.cacheStatus)
	h.POST("/api/cluster/join", s.joinClusterHandler)

	// Balancer health API
	s.registerBalancerAPI(h)

	// Volume management API
	volumes := h.Group("/volumes")
	volumes.GET("/", s.listVolumes)
	volumes.PUT("/:name", s.createVolume)
	volumes.GET("/:name", s.getVolume)
	volumes.DELETE("/:name", s.deleteVolume)
	volumes.POST("/:name/recalculate", s.recalculateVolume)
	volumes.POST("/clone", s.cloneVolume)
	volumes.POST("/:name/snapshots", s.createSnapshot)
	volumes.GET("/:name/snapshots", s.listSnapshots)
	volumes.DELETE("/:name/snapshots/:snap_id", s.deleteSnapshot)
	volumes.POST("/:name/snapshots/:snap_id/rollback", s.rollbackVolume)

	// Snapshot management API
	s.registerSnapshotAPI(h)

	// PITR (Point-in-Time Recovery) API
	s.registerPITRAPI(h)

	// Scrub health API
	s.registerScrubAPI(h)

	// Dashboard health API (badger, raft, ec status)
	s.registerDashboardHealthAPI(h)

	// Admin API for testing and operations
	s.registerAdminAPI(h)

	// Hot-reload config API
	s.registerConfigAPI(h)

	// Event log query API
	s.registerEventsAPI(h)

	// Phase 16 Week 4: alerts status + force-resend (no-op if alerts not wired).
	s.registerAlertsAPI(h)

	// Phase 16 Slice 2: heal-receipt audit API (no-op if WithReceiptAPI not set).
	s.registerReceiptAPI(h)

	// SSE event stream for dashboard
	hub := s.hub
	h.GET("/api/events", func(ctx context.Context, c *app.RequestContext) {
		c.Response.Header.Set("Content-Type", "text/event-stream")
		c.Response.Header.Set("Cache-Control", "no-cache")
		c.Response.Header.Set("Connection", "keep-alive")
		c.SetStatusCode(consts.StatusOK)

		pr, pw := io.Pipe()
		go func() {
			hub.WriteSSE(ctx, pw)
			pw.Close()
		}()
		c.Response.SetBodyStream(pr, -1)
	})

	// Phase 16: dedicated heal SSE stream. Subscribers receive only HealEvents
	// (Event.Type == "heal") so the dashboard's Self-Healing card stays
	// independent of the verbose log/metric stream.
	h.GET("/api/events/heal/stream", func(ctx context.Context, c *app.RequestContext) {
		c.Response.Header.Set("Content-Type", "text/event-stream")
		c.Response.Header.Set("Cache-Control", "no-cache")
		c.Response.Header.Set("Connection", "keep-alive")
		c.SetStatusCode(consts.StatusOK)

		pr, pw := io.Pipe()
		go func() {
			hub.WriteSSE(ctx, pw, healEvCategory)
			pw.Close()
		}()
		c.Response.SetBodyStream(pr, -1)
	})
}

// HealEmitter returns a scrubber.Emitter that fans HealEvents out to the SSE
// hub, the eventstore, and Prometheus. Callers (the serve command) wire it
// into scrubber.New via scrubber.WithEmitter.
func (s *Server) HealEmitter() scrubber.Emitter {
	return newHealEmitter(s.hub, s.emitEvent)
}
