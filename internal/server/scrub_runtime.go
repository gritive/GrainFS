package server

func (s *Server) scrubStatsSnapshot() scrubStatsResponse {
	if !s.routeFeatureAvailable(routeFeatureScrubber) {
		return scrubStatsResponse{Available: false}
	}
	stats := s.scrubber.Stats()
	return scrubStatsResponse{
		LastRun:        stats.LastRun,
		ObjectsChecked: stats.ObjectsChecked,
		ShardErrors:    stats.ShardErrors,
		Repaired:       stats.Repaired,
		Unrepairable:   stats.Unrepairable,
		Available:      true,
	}
}
