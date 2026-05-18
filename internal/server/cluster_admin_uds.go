package server

import (
	"github.com/cloudwego/hertz/pkg/app/server"
)

// RegisterClusterAdminUDS registers cluster admin routes onto the admin
// UDS Hertz instance. Reuses the same handler methods as /api/cluster/*
// on the data plane — single source of truth.
//
// Routes registered:
//   - GET  /v1/cluster/status      → clusterStatus
//   - GET  /v1/cluster/placement   → clusterPlacement
//   - POST /v1/cluster/remove-peer → removePeerHandler
//   - GET  /v1/cluster/eventlog    → queryEventLog
//
// /v1/cluster/peers is registered separately by admin.RegisterAdmin via
// the admin package's Deps interface.
//
// localhostOnly() middleware is intentionally not applied here: UDS
// connections have empty RemoteAddr (would fail isLocalhostAddr), and
// UDS file mode (0660 + admin-group) already gates access.
func (s *Server) RegisterClusterAdminUDS(h *server.Hertz) {
	g := h.Group(routePrefixAdminUDSCluster)
	g.GET(routePathAdminUDSClusterStatus, s.clusterStatus)
	g.GET(routePathAdminUDSPlacement, s.clusterPlacement)
	g.POST(routePathAdminUDSRemovePeer, s.removePeerHandler)
	g.GET(routePathAdminUDSEventLog, s.queryEventLog)
	g.POST(routePathAdminUDSTransferLeader, s.transferLeaderHandler)
	g.GET(routePathAdminUDSHealth, s.clusterHealth)
	g.GET(routePathAdminUDSBalancerStatus, s.balancerStatusHandler)
	g.GET(routePathAdminUDSCapabilities, s.capabilitiesStatus)
}
