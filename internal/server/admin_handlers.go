package server

import (
	"github.com/cloudwego/hertz/pkg/app/server"
)

// registerAdminAPI registers admin/debug endpoints for testing and operations.
// Admin endpoints require authentication or localhost access.
// (Currently no endpoints — placeholder for future debug routes.)
func (s *Server) registerAdminAPI(_ *server.Hertz) {}
