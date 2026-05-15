package server

import (
	"context"

	"github.com/gritive/GrainFS/internal/storage"
)

func (s *Server) listBucketObjects(ctx context.Context, bucket, prefix string, maxKeys int) ([]*storage.Object, error) {
	return s.ops.ListObjects(ctx, bucket, prefix, maxKeys)
}
