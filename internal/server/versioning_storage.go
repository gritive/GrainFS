package server

import (
	"context"

	"github.com/gritive/GrainFS/internal/storage"
)

func (s *Server) setBucketVersioning(bucket, status string) error {
	return s.ops.SetBucketVersioning(bucket, status)
}

func (s *Server) loadObjectVersions(ctx context.Context, bucket, prefix string, maxKeys int) ([]*storage.ObjectVersion, error) {
	ctx = s.ctxWithVersionHistory(ctx, bucket)
	return s.ops.ListObjectVersions(ctx, bucket, prefix, maxKeys)
}

func (s *Server) getBucketVersioningState(bucket string) (string, error) {
	return s.ops.GetBucketVersioning(bucket)
}
