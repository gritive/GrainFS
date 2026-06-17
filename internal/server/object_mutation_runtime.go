package server

import (
	"context"
	"io"
	"time"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/rs/zerolog/log"
)

func (s *Server) putObjectWithUserMetadataAndMD5(
	ctx context.Context,
	bucket, key string,
	body io.Reader,
	sizeHint *int64,
	contentType string,
	acl *uint8,
	userMetadata map[string]string,
	systemMetadata storage.ObjectSystemMetadata,
	contentMD5Hex string,
) (*storage.PutObjectResult, error) {
	var (
		result *storage.PutObjectResult
		err    error
	)
	ctx = s.ctxWithBucketVersioning(ctx, bucket)
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
		ContentMD5Hex:  contentMD5Hex,
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
	ctx = s.ctxWithBucketVersioning(ctx, bucket)
	result, err := s.ops.PutObjectWithResult(ctx, bucket, key, body, contentType)
	if err != nil {
		return nil, err
	}
	s.mutations.OnObjectWrite(ctx, bucket, key, result)
	return result, nil
}

// ctxWithBucketVersioning resolves the bucket's versioning state at the S3 edge
// (the same read AppendObject does) and stamps an AUTHORITATIVE decision
// (enabled OR disabled) onto the context. The per-group commit backend then
// never reads versioning itself — that read would cross the control/data-plane
// boundary (the commit backend holds no bucketver state). The decision threads
// down via context and onward over the forward wire. A read error (e.g. a
// backend that doesn't track versioning) leaves the context unstamped, so the
// commit path falls back to a local read.
func (s *Server) ctxWithBucketVersioning(ctx context.Context, bucket string) context.Context {
	if state, vErr := s.ops.GetBucketVersioning(bucket); vErr == nil {
		return cluster.ContextWithBucketVersioning(ctx, state == "Enabled")
	}
	return ctx
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
	// ctx is stamped for the DESTINATION bucket (the copy's write reaches the
	// same per-group commit path as a PUT). The SOURCE per-version read must be
	// gated by the SOURCE bucket's versioning decision, so re-stamp a separate
	// ctx for the source bucket and thread it down for the source read only.
	req.SourceVersioningCtx = s.ctxWithBucketVersioning(ctx, req.Source.Bucket)
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
