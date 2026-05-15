package server

import (
	"bytes"
	"context"
	"fmt"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

func (s *Server) handlePut(ctx context.Context, c *app.RequestContext) {
	if s.isDegraded() {
		writeXMLError(c, consts.StatusServiceUnavailable, "ServiceUnavailable", "system is in degraded mode: writes suspended")
		return
	}

	bucket := c.Param("bucket")
	key := getKey(c)

	// Check if this is an UploadPart request
	uploadID := string(c.QueryArgs().Peek("uploadId"))
	partNumberStr := string(c.QueryArgs().Peek("partNumber"))
	if uploadID != "" && partNumberStr != "" {
		s.uploadPart(ctx, c, bucket, key, uploadID, partNumberStr)
		return
	}

	// Check for CopyObject (PUT with x-amz-copy-source header)
	copySource := string(c.GetHeader("x-amz-copy-source"))
	if copySource != "" {
		s.handleCopyObject(ctx, c, bucket, key, copySource)
		return
	}

	contentType := putObjectContentType(c)
	rawBody, err := putObjectBody(c)
	if err != nil {
		c.AbortWithMsg(err.Error(), consts.StatusBadRequest)
		return
	}

	body := bytes.NewReader(rawBody)
	userMetadata := copyUserMetadata(c)

	result, putErr := s.putObjectWithUserMetadata(ctx, bucket, key, body, contentType, putObjectACL(c), userMetadata)
	if putErr != nil {
		mapError(c, putErr)
		return
	}
	obj := result.Object

	c.Header("ETag", fmt.Sprintf("\"%s\"", obj.ETag))
	if obj.VersionID != "" {
		c.Header("X-Amz-Version-Id", obj.VersionID)
	}
	c.Status(consts.StatusOK)
}

func (s *Server) deleteObject(ctx context.Context, c *app.RequestContext) {
	if s.isDegraded() {
		writeXMLError(c, consts.StatusServiceUnavailable, "ServiceUnavailable", "system is in degraded mode: writes suspended")
		return
	}

	bucket := c.Param("bucket")
	key := getKey(c)

	// DELETE /:bucket/:key?uploadId=<id> — abort an in-progress multipart upload.
	// Checked before ?versionId= because S3 routes the request to AbortMultipartUpload
	// when uploadId is present, regardless of any other query string.
	if uploadID := string(c.QueryArgs().Peek("uploadId")); uploadID != "" {
		if err := s.abortMultipartSession(ctx, bucket, key, uploadID); err != nil {
			mapError(c, err)
			return
		}
		c.Status(consts.StatusNoContent)
		return
	}

	// DELETE /:bucket/:key?versionId=<id> — hard-delete specific version
	if versionID := string(c.QueryArgs().Peek("versionId")); versionID != "" {
		if err := s.deleteObjectVersion(bucket, key, versionID); err != nil {
			mapError(c, err)
			return
		}
		c.Status(consts.StatusNoContent)
		return
	}

	result, err := s.deleteObjectWithMutation(ctx, bucket, key)
	if err != nil {
		mapError(c, err)
		return
	}
	if result.Deleted.DeleteMarker {
		c.Header("x-amz-delete-marker", "true")
		c.Header("x-amz-version-id", result.Deleted.VersionID)
	}
	c.Status(consts.StatusNoContent)
}
