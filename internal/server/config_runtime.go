package server

import "time"

func (s *Server) setScrubInterval(interval time.Duration) {
	if s.scrubber != nil {
		s.scrubber.SetInterval(interval)
	}
}
