package server

import "github.com/gritive/GrainFS/internal/server/alertssvc"

// Alerts returns the alerts state wired into this server, or nil if alerts
// were not configured. Used by other server components (raft monitor, disk
// collector) to push fault/healthy reports.
func (s *Server) Alerts() *alertssvc.State { return s.alerts }
