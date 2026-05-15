package server

func (s *Server) alertsStatusSnapshot() alertsStatusResponse {
	return s.alerts.StatusSnapshot()
}

func (s *Server) resendLastFailedAlert() (bool, error) {
	return s.alerts.ResendLastFailed()
}
