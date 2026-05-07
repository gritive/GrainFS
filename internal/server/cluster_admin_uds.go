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
	g := h.Group("/v1/cluster")
	g.GET("/status", s.clusterStatus)
	g.POST("/remove-peer", s.removePeerHandler)
	g.GET("/eventlog", s.queryEventLog)
}
