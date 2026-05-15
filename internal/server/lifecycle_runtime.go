package server

import (
	"context"
	"errors"

	"github.com/gritive/GrainFS/internal/lifecycle"
)

type lifecycleBucketLookupError struct {
	err error
}

func (e lifecycleBucketLookupError) Error() string {
	return e.err.Error()
}

func (e lifecycleBucketLookupError) Unwrap() error {
	return e.err
}

func isLifecycleBucketLookupError(err error) bool {
	var lookupErr lifecycleBucketLookupError
	return errors.As(err, &lookupErr)
}

func (s *Server) applyBucketLifecycle(ctx context.Context, bucket string, body []byte) error {
	if err := s.ops.HeadBucket(ctx, bucket); err != nil {
		return lifecycleBucketLookupError{err: err}
	}
	return s.lifecycle.Apply(ctx, bucket, body)
}

func (s *Server) loadBucketLifecycleRaw(bucket string) ([]byte, error) {
	return s.lifecycle.GetRaw(bucket)
}

func (s *Server) removeBucketLifecycle(ctx context.Context, bucket string) error {
	return s.lifecycle.Delete(ctx, bucket)
}

func (s *Server) lifecycleStatusSnapshot() lifecycle.Status {
	return s.lifecycle.Status()
}
