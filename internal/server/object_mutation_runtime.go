package server

import (
	"context"
	"io"

	"github.com/gritive/GrainFS/internal/storage"
)

func (s *Server) putObjectWithUserMetadata(
	ctx context.Context,
	bucket, key string,
	body io.Reader,
	contentType string,
	acl *uint8,
	userMetadata map[string]string,
) (*storage.PutObjectResult, error) {
	var (
		result *storage.PutObjectResult
		err    error
	)
	if acl != nil {
		result, err = s.ops.PutObjectWithACLAndUserMetadataResult(ctx, bucket, key, body, contentType, *acl, userMetadata)
	} else {
		result, err = s.ops.PutObjectWithUserMetadataResult(ctx, bucket, key, body, contentType, userMetadata)
	}
	if err != nil {
		return nil, err
	}
	s.mutations.OnObjectWrite(ctx, bucket, key, result)
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
