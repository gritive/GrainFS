package server

import (
	"context"

	"github.com/gritive/GrainFS/internal/storage"
)

func (s *Server) listBucketObjectsPage(ctx context.Context, bucket, prefix, marker string, maxKeys int) ([]*storage.Object, bool, error) {
	return s.ops.ListObjectsPage(ctx, bucket, prefix, marker, maxKeys)
}
