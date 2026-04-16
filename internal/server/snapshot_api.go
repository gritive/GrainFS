package server

// SnapshotRequest represents a snapshot creation request.
type SnapshotRequest struct {
	Reason string `json:"reason,omitempty"`
}

// SnapshotResponse represents the snapshot creation response.
type SnapshotResponse struct {
	Index     uint64 `json:"index"`
	Term      uint64 `json:"term"`
	Timestamp string `json:"timestamp"`
	Size      int64  `json:"size"`
}

// RestoreRequest represents a snapshot restore request.
type RestoreRequest struct {
	Index uint64 `json:"index"`
}

// registerSnapshotAPI registers snapshot management endpoints.
func (s *Server) registerSnapshotAPI() {
	// For now, this is a placeholder - full integration would require access to Hertz router
	// The actual implementation would be integrated into registerRoutes with h.POST/GET
	// This provides the structure for future implementation
}
