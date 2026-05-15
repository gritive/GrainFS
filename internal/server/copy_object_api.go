package server

import (
	"context"
	"fmt"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/s3auth"
	"github.com/gritive/GrainFS/internal/storage"
)

// handleCopyObject processes PUT with x-amz-copy-source header (S3 CopyObject).
func (s *Server) handleCopyObject(ctx context.Context, c *app.RequestContext, dstBucket, dstKey, copySource string) {
	src, ok := parseCopySource(copySource)
	if !ok {
		writeXMLError(c, consts.StatusBadRequest, "InvalidArgument", "invalid x-amz-copy-source format")
		return
	}

	// Source GetObject authorization. Middleware already gated the destination
	// at PhasePreLoad (PUT to dst). The source bucket needs its own
	// authorization chain: pre-load (IAM + bucket policy) before we touch
	// storage so an unauthorized caller cannot probe object existence via
	// HeadObject, then post-load (ACL) after the source object is loaded.
	if s.mustAuthorize(ctx, c, src.Bucket, src.Key, s3auth.GetObject) {
		return
	}
	srcObj, srcErr := s.loadCopySourceObject(ctx, src)
	if srcErr != nil {
		mapError(c, srcErr)
		return
	}
	if s.mustAuthorizePostLoad(ctx, c, src.Bucket, src.Key, s3auth.GetObject, srcObj.ACL) {
		return
	}

	var acl *uint8
	if aclHeader := string(c.GetHeader("x-amz-acl")); aclHeader != "" {
		parsed := uint8(s3auth.ParseACLHeader(aclHeader))
		acl = &parsed
	}
	directive, ok := parseCopyMetadataDirective(string(c.GetHeader("x-amz-metadata-directive")))
	if !ok {
		writeXMLError(c, consts.StatusBadRequest, "InvalidArgument", "invalid x-amz-metadata-directive")
		return
	}
	preconditions, ok := copyPreconditions(c)
	if !ok {
		writeXMLError(c, consts.StatusBadRequest, "InvalidArgument", "invalid copy source condition date")
		return
	}
	req := storage.CopyObjectRequest{
		Source:            src,
		Destination:       storage.ObjectRef{Bucket: dstBucket, Key: dstKey},
		ACL:               acl,
		MetadataDirective: directive,
		ContentType:       string(c.GetHeader("Content-Type")),
		UserMetadata:      copyUserMetadata(c),
		Preconditions:     preconditions,
	}
	result, err := s.copyObjectWithMutation(ctx, req)
	if err != nil {
		mapError(c, err)
		return
	}
	obj := result.Object

	response := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<CopyObjectResult>
  <ETag>"%s"</ETag>
  <LastModified>%s</LastModified>
</CopyObjectResult>`, obj.ETag, time.Unix(obj.LastModified, 0).UTC().Format(time.RFC3339))
	c.Data(consts.StatusOK, "application/xml", []byte(response))
}
