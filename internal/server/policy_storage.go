package server

func (s *Server) storeBucketPolicy(bucket string, body []byte) error {
	return s.ops.SetBucketPolicy(bucket, body)
}

func (s *Server) loadBucketPolicy(bucket string) ([]byte, error) {
	return s.ops.GetBucketPolicy(bucket)
}

func (s *Server) deleteBucketPolicyStorage(bucket string) error {
	return s.ops.DeleteBucketPolicy(bucket)
}
