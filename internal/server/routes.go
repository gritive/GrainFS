package server

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func (s *Server) registerRoutes(h *server.Hertz) {
	s.registerMetricsAPI(h)
	s.registerDashboardUI(h)
	s.registerIcebergAPI(h)
	s.registerS3API(h)
	s.registerClusterAPI(h)
	s.registerBalancerAPI(h)
	s.registerLifecycleStatusAPI(h)
	s.registerSnapshotAPI(h)
	s.registerRaftSnapshotAPI(h)
	s.registerPITRAPI(h)
	s.registerScrubAPI(h)
	s.registerDashboardHealthAPI(h)
	s.registerAdminAPI(h)
	s.registerConfigAPI(h)
	s.registerEventsAPI(h)
	s.registerAuditAPI(h)
	s.registerAlertsAPI(h)
	s.registerReceiptAPI(h)
	s.registerIncidentAPI(h)
}

func (s *Server) registerMetricsAPI(h *server.Hertz) {
	promHandler := promhttp.Handler()
	h.GET(routePathMetrics, func(_ context.Context, c *app.RequestContext) {
		promHandler.ServeHTTP(newResponseWriter(c), toHTTPRequest(c))
	})
}

func (s *Server) registerDashboardUI(h *server.Hertz) {
	h.GET(routePathUIRoot, s.serveDashboard)
}

func (s *Server) registerS3API(h *server.Hertz) {
	h.GET(routePathS3Root, s.listBuckets)

	h.PUT(routePathS3Bucket, s.createBucket)
	h.HEAD(routePathS3Bucket, s.headBucket)
	h.DELETE(routePathS3Bucket, s.deleteBucket)
	h.GET(routePathS3Bucket, s.listObjects)
	h.POST(routePathS3Bucket, s.handlePost)

	h.PUT(routePathS3Object, s.handlePut)
	h.GET(routePathS3Object, s.getObject)
	h.HEAD(routePathS3Object, s.headObject)
	h.DELETE(routePathS3Object, s.deleteObject)
	h.POST(routePathS3Object, s.handlePost)
}

func (s *Server) registerClusterAPI(h *server.Hertz) {
	h.GET(routePathClusterStatus, s.clusterStatus)
	h.GET(routePathClusterPlacement, s.clusterPlacement)
	h.GET(routePathCacheStatus, s.cacheStatus)
	h.POST(routePathClusterJoin, s.joinClusterHandler)
	h.POST(routePathClusterRemovePeer, localhostOnly(), s.removePeerHandler)
}
