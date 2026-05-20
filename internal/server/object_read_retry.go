package server

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/gritive/GrainFS/internal/storage"
)

const (
	defaultReadAfterWriteRetryTimeout  = 500 * time.Millisecond
	defaultReadAfterWriteRetryInterval = 10 * time.Millisecond
)

func (s *Server) getObjectWithReadAfterWriteRetry(ctx context.Context, bucket, key string) (io.ReadCloser, *storage.Object, error) {
	result, err := retryReadAfterWrite(ctx, s.readAfterWriteRetryTimeout, s.readAfterWriteRetryInterval, func() (objectReadResult, error) {
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
	return retryReadAfterWrite(ctx, s.readAfterWriteRetryTimeout, s.readAfterWriteRetryInterval, func() (*storage.Object, error) {
		return s.ops.HeadObject(ctx, bucket, key)
	})
}

type objectReadResult struct {
	body   io.ReadCloser
	object *storage.Object
}

func retryReadAfterWrite[T any](ctx context.Context, timeout, interval time.Duration, lookup func() (T, error)) (T, error) {
	result, err := lookup()
	if timeout <= 0 || !errors.Is(err, storage.ErrObjectNotFound) {
		return result, err
	}

	deadline := time.Now().Add(timeout)
	var zero T
	lastErr := err
	for {
		if time.Now().After(deadline) {
			return zero, lastErr
		}
		if !sleepReadAfterWriteRetry(ctx, interval) {
			return zero, ctx.Err()
		}
		result, err = lookup()
		if !errors.Is(err, storage.ErrObjectNotFound) {
			return result, err
		}
		lastErr = err
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

func sleepReadAfterWriteRetry(ctx context.Context, interval time.Duration) bool {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
