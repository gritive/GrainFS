package server

import (
	"context"

	"github.com/gritive/GrainFS/internal/metrics"
)

func (s *Server) createS3Bucket(ctx context.Context, bucket string) error {
	if err := s.ops.CreateBucket(ctx, bucket); err != nil {
		return err
	}
	metrics.BucketsTotal.Inc()
	s.issueCreatorGrant(ctx, bucket)
	s.mutations.OnBucketCreate(ctx, bucket)
	return nil
}

func (s *Server) deleteS3Bucket(ctx context.Context, bucket string) error {
	if err := s.ops.DeleteBucket(ctx, bucket); err != nil {
		return err
	}
	metrics.BucketsTotal.Dec()
	s.mutations.OnBucketDelete(ctx, bucket)
	return nil
}
