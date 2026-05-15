package server

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/gritive/GrainFS/internal/storage"
)

const (
	readAfterWriteRetryTimeout  = 500 * time.Millisecond
	readAfterWriteRetryInterval = 10 * time.Millisecond
)

func (s *Server) getObjectWithReadAfterWriteRetry(ctx context.Context, bucket, key string) (io.ReadCloser, *storage.Object, error) {
	result, err := retryReadAfterWrite(ctx, func() (objectReadResult, error) {
		rc, obj, err := s.ops.GetObject(ctx, bucket, key)
		return objectReadResult{body: rc, object: obj}, err
	})
	return result.body, result.object, err
}

func (s *Server) loadObjectForGet(ctx context.Context, bucket, key, versionID string) (io.ReadCloser, *storage.Object, error) {
	if versionID != "" {
		return s.ops.GetObjectVersion(bucket, key, versionID)
	}
	return s.getObjectWithReadAfterWriteRetry(ctx, bucket, key)
}

func (s *Server) headObjectWithReadAfterWriteRetry(ctx context.Context, bucket, key string) (*storage.Object, error) {
	return retryReadAfterWrite(ctx, func() (*storage.Object, error) {
		return s.ops.HeadObject(ctx, bucket, key)
	})
}

type objectReadResult struct {
	body   io.ReadCloser
	object *storage.Object
}

func retryReadAfterWrite[T any](ctx context.Context, lookup func() (T, error)) (T, error) {
	deadline := time.Now().Add(readAfterWriteRetryTimeout)
	var zero T
	var lastErr error
	for {
		result, err := lookup()
		if !errors.Is(err, storage.ErrObjectNotFound) {
			return result, err
		}
		lastErr = err
		if time.Now().After(deadline) {
			return zero, lastErr
		}
		if !sleepReadAfterWriteRetry(ctx) {
			return zero, ctx.Err()
		}
	}
}

func (s *Server) loadObjectForHead(ctx context.Context, bucket, key, versionID string) (*storage.Object, error) {
	if versionID != "" {
		return s.ops.HeadObjectVersion(bucket, key, versionID)
	}
	return s.headObjectWithReadAfterWriteRetry(ctx, bucket, key)
}

func (s *Server) loadCopySourceObject(ctx context.Context, src storage.ObjectRef) (*storage.Object, error) {
	return s.ops.HeadObject(ctx, src.Bucket, src.Key)
}

func sleepReadAfterWriteRetry(ctx context.Context) bool {
	timer := time.NewTimer(readAfterWriteRetryInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
