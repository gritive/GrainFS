package server

import "github.com/gritive/GrainFS/internal/storage"

func (s *Server) setBucketVersioning(bucket, status string) error {
	return s.ops.SetBucketVersioning(bucket, status)
}

func (s *Server) loadObjectVersions(bucket, prefix string, maxKeys int) ([]*storage.ObjectVersion, error) {
	return s.ops.ListObjectVersions(bucket, prefix, maxKeys)
}

func (s *Server) getBucketVersioningState(bucket string) (string, error) {
	return s.ops.GetBucketVersioning(bucket)
}
