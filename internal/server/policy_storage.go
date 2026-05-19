package server

func (s *Server) loadBucketPolicy(bucket string) ([]byte, error) {
	return s.ops.GetBucketPolicy(bucket)
}
