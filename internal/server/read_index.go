package server

import "context"

func (s *Server) waitForReadIndex(ctx context.Context) error {
	if s.readIndexer == nil {
		return nil
	}
	readIdx, err := s.readIndexer.ReadIndex(ctx)
	if err != nil {
		return err
	}
	return s.readIndexer.WaitApplied(ctx, readIdx)
}
