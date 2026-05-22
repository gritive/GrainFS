package server

import (
	"context"
	"io"
	"time"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/rs/zerolog/log"
)

func (s *Server) putObjectWithUserMetadata(
	ctx context.Context,
	bucket, key string,
	body io.Reader,
	sizeHint *int64,
	contentType string,
	acl *uint8,
	userMetadata map[string]string,
	systemMetadata storage.ObjectSystemMetadata,
) (*storage.PutObjectResult, error) {
	var (
		result *storage.PutObjectResult
		err    error
	)
	backendStart := time.Now()
	result, err = s.ops.PutObjectWithRequestResult(ctx, storage.PutObjectRequest{
		Bucket:         bucket,
		Key:            key,
		Body:           body,
		SizeHint:       sizeHint,
		ContentType:    contentType,
		ACL:            acl,
		UserMetadata:   userMetadata,
		SystemMetadata: systemMetadata,
	})
	cluster.ObservePutTraceStage(ctx, cluster.PutTraceStageHTTPPutBackend, backendStart, cluster.PutTraceStageFields{})
	if err != nil {
		log.Warn().
			Err(err).
			Str("bucket", bucket).
			Str("key", key).
			Str("content_type", contentType).
			Bool("has_acl", acl != nil).
			Bool("has_sse", systemMetadata.SSEAlgorithm != "").
			Msg("s3 put: backend mutation failed")
		return nil, err
	}
	mutationStart := time.Now()
	s.mutations.OnObjectWrite(ctx, bucket, key, result)
	cluster.ObservePutTraceStage(ctx, cluster.PutTraceStageHTTPPutMutation, mutationStart, cluster.PutTraceStageFields{})
	return result, nil
}

func (s *Server) putFormObject(ctx context.Context, bucket, key string, body io.Reader, contentType string) (*storage.PutObjectResult, error) {
	result, err := s.ops.PutObjectWithResult(ctx, bucket, key, body, contentType)
	if err != nil {
		return nil, err
	}
	s.mutations.OnObjectWrite(ctx, bucket, key, result)
	return result, nil
}

func (s *Server) completeMultipartObject(ctx context.Context, bucket, key, uploadID string, parts []storage.Part) (*storage.PutObjectResult, error) {
	result, err := s.ops.CompleteMultipartUploadWithResult(ctx, bucket, key, uploadID, parts)
	if err != nil {
		return nil, err
	}
	s.mutations.OnObjectWrite(ctx, bucket, key, result)
	return result, nil
}

func (s *Server) copyObjectWithMutation(ctx context.Context, req storage.CopyObjectRequest) (*storage.CopyObjectResult, error) {
	result, err := s.ops.CopyObject(ctx, req)
	if err != nil {
		return nil, err
	}
	s.mutations.OnObjectCopy(ctx, req.Source.Bucket, req.Source.Key, req.Destination.Bucket, req.Destination.Key, result)
	return result, nil
}

func (s *Server) deleteObjectWithMutation(ctx context.Context, bucket, key string) (*storage.DeleteObjectResult, error) {
	result, err := s.ops.DeleteObjectWithResult(ctx, bucket, key)
	if err != nil {
		return nil, err
	}
	s.mutations.OnObjectDelete(ctx, bucket, key, result)
	return result, nil
}

func (s *Server) deleteObjectVersion(bucket, key, versionID string) error {
	return s.ops.DeleteObjectVersion(bucket, key, versionID)
}
