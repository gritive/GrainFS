package server

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/storage/tagging"
)

// handlePost routes POST requests for multipart upload operations.
func (s *Server) handlePost(ctx context.Context, c *app.RequestContext) {
	if s.isDegraded() {
		writeXMLError(c, consts.StatusServiceUnavailable, "ServiceUnavailable", "system is in degraded mode: writes suspended")
		return
	}

	bucket := c.Param("bucket")
	key := getKey(c)

	// POST /:bucket?delete -> DeleteObjects
	if key == "" && c.QueryArgs().Has("delete") {
		s.deleteObjects(ctx, c, bucket)
		return
	}

	// POST /:bucket/:key?uploads -> CreateMultipartUpload
	if c.QueryArgs().Has("uploads") {
		s.createMultipartUpload(ctx, c, bucket, key)
		return
	}

	// POST /:bucket/:key?uploadId=xxx -> CompleteMultipartUpload
	uploadID := string(c.QueryArgs().Peek("uploadId"))
	if uploadID != "" {
		s.completeMultipartUpload(ctx, c, bucket, key, uploadID)
		return
	}

	// POST /:bucket with multipart/form-data -> Form-based Upload (POST Policy)
	ct := string(c.GetHeader("Content-Type"))
	if strings.HasPrefix(ct, "multipart/form-data") {
		s.handleFormUpload(ctx, c, bucket)
		return
	}

	writeXMLError(c, consts.StatusBadRequest, "InvalidRequest", "unsupported POST operation")
}

func (s *Server) createMultipartUpload(ctx context.Context, c *app.RequestContext, bucket, key string) {
	contentType := string(c.GetHeader("Content-Type"))
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	var tags []storage.Tag
	if raw := string(c.GetHeader("x-amz-tagging")); raw != "" {
		parsed, err := ParseTaggingHeader(raw)
		if err != nil {
			writeXMLError(c, consts.StatusBadRequest, "InvalidTag", err.Error())
			return
		}
		if err := tagging.Validate(parsed); err != nil {
			writeXMLError(c, consts.StatusBadRequest, "InvalidTag", err.Error())
			return
		}
		tags = parsed
	}

	var (
		upload *storage.MultipartUpload
		err    error
	)
	if len(tags) > 0 {
		upload, err = s.createMultipartSessionWithTags(ctx, bucket, key, contentType, tags)
	} else {
		upload, err = s.createMultipartSession(ctx, bucket, key, contentType)
	}
	if err != nil {
		mapError(c, err)
		return
	}

	writeInitiateMultipartUploadResult(c, bucket, key, upload)
}

func (s *Server) uploadPart(ctx context.Context, c *app.RequestContext, bucket, key, uploadID, partNumberStr string) {
	partNumber, err := strconv.Atoi(partNumberStr)
	if err != nil || partNumber < 1 {
		writeXMLError(c, consts.StatusBadRequest, "InvalidArgument", "invalid part number")
		return
	}

	// UploadPart bodies use the same SigV4 streaming / aws-chunked transport as
	// PutObject. Strip the chunk framing here so the part file stores the
	// caller's plaintext bytes — leaving the framing in place inflates Part.Size
	// and corrupts ?partNumber=N GET responses (warp's multipart workload sends
	// every part with `X-Amz-Content-Sha256: STREAMING-AWS4-HMAC-SHA256-PAYLOAD`).
	bodyBytes, err := putObjectBody(c)
	if err != nil {
		writeXMLError(c, consts.StatusBadRequest, "InvalidArgument", err.Error())
		return
	}
	body := bytes.NewReader(bodyBytes)
	part, err := s.uploadMultipartPart(ctx, bucket, key, uploadID, partNumber, body)
	if err != nil {
		mapError(c, err)
		return
	}

	c.Header("ETag", fmt.Sprintf("\"%s\"", part.ETag))
	c.Status(consts.StatusOK)
}

func (s *Server) completeMultipartUpload(ctx context.Context, c *app.RequestContext, bucket, key, uploadID string) {
	parts, err := parseCompleteMultipartUpload(c.Request.Body())
	if err != nil {
		writeXMLError(c, consts.StatusBadRequest, "MalformedXML", "invalid XML body")
		return
	}

	result, err := s.completeMultipartObject(ctx, bucket, key, uploadID, parts)
	if err != nil {
		mapError(c, err)
		return
	}
	writeCompleteMultipartUploadResult(c, bucket, key, result.Object)
}
