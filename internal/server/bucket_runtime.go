package server

import (
	"context"

	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/storage"
)

func (s *Server) visibleBuckets(ctx context.Context) ([]string, error) {
	buckets, err := s.ops.ListBuckets(ctx)
	if err != nil {
		return nil, err
	}

	if scope := iam.ScopeFromContext(ctx); len(scope) > 0 {
		filtered := buckets[:0]
		for _, name := range buckets {
			if iam.ScopeAllows(scope, name) {
				filtered = append(filtered, name)
			}
		}
		buckets = filtered
	}

	filtered := buckets[:0]
	for _, name := range buckets {
		if !storage.IsInternalBucket(name) {
			filtered = append(filtered, name)
		}
	}
	return filtered, nil
}

func (s *Server) requireBucket(ctx context.Context, bucket string) error {
	return s.ops.HeadBucket(ctx, bucket)
}
