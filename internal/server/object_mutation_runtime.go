package server

import (
	"context"
	"errors"
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
	if ctx, err = s.ctxWithBucketVersioningStrict(ctx, bucket); err != nil {
		return nil, err
	}
	backendStart := time.Now()
	result, err = s.ops.PutObjectWithRequestResult(ctx, storage.PutObjectRequest{
		Bucket:         bucket,
		Key:            key,
		Body:           body,
		SizeHint:       sizeHint,
		SizeHintExact:  sizeHint != nil,
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

// putFormObject persists an S3 POST form upload. The multipart FileHeader.Size
// is authoritative, so thread it as an EXACT SizeHint: the cluster backend's
// single streaming write path requires SizeHintExact (it errors without one),
// and an exact size is strictly fine on the single-node LocalBackend too.
func (s *Server) putFormObject(ctx context.Context, bucket, key string, body io.Reader, contentType string, size int64) (*storage.PutObjectResult, error) {
	ctx, err := s.ctxWithBucketVersioningStrict(ctx, bucket)
	if err != nil {
		return nil, err
	}
	result, err := s.ops.PutObjectWithRequestResult(ctx, storage.PutObjectRequest{
		Bucket:        bucket,
		Key:           key,
		Body:          body,
		ContentType:   contentType,
		SizeHint:      &size,
		SizeHintExact: true,
	})
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
// down via context and onward over the forward wire.
//
// TOLERANT variant (READ paths: GET/HEAD/LIST + Copy source): uses the PLAIN
// local read — never the linearizing barrier. Reads must stay available and
// instant even when group-0 (the control plane) has no leader; a slightly-stale
// versioning view only affects read-mode selection, never data integrity. A
// resolve error leaves the context unstamped so the read falls back to a local
// read. MUTATING paths use ctxWithBucketVersioningStrict instead.
func (s *Server) ctxWithBucketVersioning(ctx context.Context, bucket string) context.Context {
	if state, vErr := s.ops.GetBucketVersioning(bucket); vErr == nil {
		return cluster.ContextWithBucketVersioning(ctx, state == "Enabled")
	}
	return ctx
}

// ctxWithBucketVersioningStrict is the LINEARIZING variant for MUTATING paths
// (PUT / CompleteMultipart / Copy-dst): a follower must not read a stale local
// replica and mis-version the write. It uses GetBucketVersioningLinearized
// (ReadIndex+WaitApplied), which DEGRADES to a local read during a group-0
// leaderless window (so writes aren't coupled to control-plane leadership) and
// only returns an error on a genuine resolve fault — which the caller surfaces
// rather than silently writing non-versioned.
func (s *Server) ctxWithBucketVersioningStrict(ctx context.Context, bucket string) (context.Context, error) {
	state, err := s.ops.GetBucketVersioningLinearized(ctx, bucket)
	if err != nil {
		// A backend that doesn't track versioning is not a fault — leave the ctx
		// unstamped (downstream treats it as unversioned), exactly as the tolerant
		// path does. A genuine resolve fault (e.g. local store error) surfaces so
		// the write doesn't silently go non-versioned.
		var unsupported storage.UnsupportedOperationError
		if errors.As(err, &unsupported) {
			return ctx, nil
		}
		return ctx, err
	}
	return cluster.ContextWithBucketVersioning(ctx, state == "Enabled"), nil
}

// ctxWithVersionHistory stamps "this bucket can hold version history" =
// versioning is Enabled OR Suspended (a Suspended bucket retains every version
// created while Enabled). Distinct from ctxWithBucketVersioning, which stamps
// Enabled-only for the latest-only ListObjects derive. Same predicate S4a uses
// (per_version_cutover_verify.go:172). The stamped ctx flows only into
// ListObjectVersions, so the bool's "history-bearing" meaning here never
// collides with the Enabled-only meaning on the ListObjects path.
func (s *Server) ctxWithVersionHistory(ctx context.Context, bucket string) context.Context {
	if state, err := s.ops.GetBucketVersioning(bucket); err == nil {
		return cluster.ContextWithBucketVersioning(ctx, state == "Enabled" || state == "Suspended")
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
