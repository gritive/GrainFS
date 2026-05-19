package server

import (
	"context"
	"io"

	"github.com/gritive/GrainFS/internal/storage"
)

func (s *Server) createMultipartSession(ctx context.Context, bucket, key, contentType string) (*storage.MultipartUpload, error) {
	return s.ops.CreateMultipartUpload(ctx, bucket, key, contentType)
}

func (s *Server) createMultipartSessionWithTags(ctx context.Context, bucket, key, contentType string, tags []storage.Tag) (*storage.MultipartUpload, error) {
	return s.ops.CreateMultipartUploadWithTags(ctx, bucket, key, contentType, tags)
}

func (s *Server) uploadMultipartPart(ctx context.Context, bucket, key, uploadID string, partNumber int, body io.Reader) (*storage.Part, error) {
	return s.ops.UploadPart(ctx, bucket, key, uploadID, partNumber, body)
}

func (s *Server) abortMultipartSession(ctx context.Context, bucket, key, uploadID string) error {
	return s.ops.AbortMultipartUpload(ctx, bucket, key, uploadID)
}

func (s *Server) listMultipartSessions(ctx context.Context, bucket, prefix string, maxUploads int) ([]*storage.MultipartUpload, error) {
	return s.ops.ListMultipartUploads(ctx, bucket, prefix, maxUploads)
}

func (s *Server) listMultipartParts(ctx context.Context, bucket, key, uploadID string, maxParts int) ([]storage.Part, error) {
	return s.ops.ListParts(ctx, bucket, key, uploadID, maxParts)
}
