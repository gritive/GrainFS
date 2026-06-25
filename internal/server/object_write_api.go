package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/storage/tagging"
)

func (s *Server) handlePut(ctx context.Context, c *app.RequestContext) {
	requestStart := time.Now()
	if s.isDegraded() {
		writeXMLError(c, consts.StatusServiceUnavailable, "ServiceUnavailable", "system is in degraded mode: writes suspended")
		return
	}

	bucket := c.Param("bucket")
	key := getKey(c)
	if key == "" {
		s.createBucket(ctx, c)
		return
	}

	if c.QueryArgs().Has("tagging") {
		s.putObjectTagging(ctx, c)
		return
	}
	if c.QueryArgs().Has("retention") {
		s.putObjectRetention(ctx, c)
		return
	}
	// PUT /:bucket/:key?legal-hold has no dedicated route; without this dispatch it
	// falls through to a plain PutObject and OVERWRITES the object body with the
	// legal-hold XML (silent data corruption). Reject with 501. Placed after the
	// isDegraded guard above so degraded nodes still return 503, and before the
	// PutObject fall-through so the object is never mutated.
	if c.QueryArgs().Has("legal-hold") {
		s.putObjectLegalHold(ctx, c)
		return
	}

	// Check if this is an UploadPart / UploadPartCopy request. Both carry
	// uploadId+partNumber; UploadPartCopy is distinguished by x-amz-copy-source.
	// This copy-source check MUST stay INSIDE the uploadId+partNumber branch:
	// the standalone CopyObject check below runs only for non-part requests, so
	// without this fork an UploadPartCopy is silently handled as a plain
	// UploadPart and the copy source is dropped (empty part stored).
	uploadID := string(c.QueryArgs().Peek("uploadId"))
	partNumberStr := string(c.QueryArgs().Peek("partNumber"))
	if uploadID != "" && partNumberStr != "" {
		if copySource := string(c.GetHeader("x-amz-copy-source")); copySource != "" {
			s.uploadPartCopy(ctx, c, bucket, key, uploadID, partNumberStr, copySource)
			return
		}
		s.uploadPart(ctx, c, bucket, key, uploadID, partNumberStr)
		return
	}

	// Check for CopyObject (PUT with x-amz-copy-source header)
	copySource := string(c.GetHeader("x-amz-copy-source"))
	if copySource != "" {
		s.handleCopyObject(ctx, c, bucket, key, copySource)
		return
	}

	// AppendObject (S3 Express): PUT with x-amz-write-offset-bytes header.
	// Header absence falls through to the normal PutObject path.
	if s.appendObject(ctx, c, bucket, key) {
		return
	}

	sizeClass := cluster.PutTraceSizeUnknown
	if contentLength := c.Request.Header.ContentLength(); contentLength >= 0 {
		if contentLength > cluster.DefaultMaxForwardBodyBytes {
			sizeClass = cluster.PutTraceSizeLarge
		} else {
			sizeClass = cluster.PutTraceSizeSmall
		}
	}
	ctx = cluster.ContextWithPutTrace(ctx, cluster.PutTraceRequest{
		Bucket:      bucket,
		Key:         key,
		Ingress:     cluster.PutTraceIngressLocalLeader,
		SizeClass:   sizeClass,
		ForwardMode: cluster.PutTraceForwardNone,
	})
	defer func() {
		cluster.ObservePutTraceStage(ctx, cluster.PutTraceStageHTTPPutTotal, requestStart, cluster.PutTraceStageFields{})
	}()

	// PutObject carrying Object Lock headers (x-amz-object-lock-*) requests WORM
	// semantics we do not implement; today they are silently ignored and the PUT
	// succeeds (false success). Reject with 501 (fail-closed) on the plain
	// PutObject path only — multipart/copy/append have already returned above.
	if hasObjectLockHeaders(c) {
		writeObjectLockNotImplemented(c)
		return
	}

	prepareStart := time.Now()
	systemMetadata, sseErr := parseObjectSSEHeaders(c)
	if sseErr != nil {
		writeSSEHeaderError(c, sseErr)
		return
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

	contentType := putObjectContentType(c)
	var body io.Reader
	var sizeHint *int64
	bodyBytes := int64(0)
	if putObjectShouldStream(c) {
		stream, err := putObjectPayloadReader(c)
		if err != nil {
			c.AbortWithMsg(err.Error(), consts.StatusBadRequest)
			return
		}
		// aws-chunked wire Content-Length includes per-chunk framing; the decoded
		// stream yields only the object bytes, so the exact-length reader must
		// enforce the decoded size (else it short-reads and fails the PUT).
		streamLength := putObjectStreamLength(c)
		sizeHint = &streamLength
		bodyBytes = streamLength
		body = newExactLengthReader(stream, streamLength)
	} else {
		rawBody, err := putObjectBody(c)
		if err != nil {
			c.AbortWithMsg(err.Error(), consts.StatusBadRequest)
			return
		}
		bodyBytes = int64(len(rawBody))
		sizeHint = &bodyBytes
		body = bytes.NewReader(rawBody)
	}
	userMetadata := copyUserMetadata(c)
	cluster.ObservePutTraceStage(ctx, cluster.PutTraceStageHTTPPutPrepare, prepareStart, cluster.PutTraceStageFields{
		Bytes: bodyBytes,
	})

	contentMD5Hex, md5Err := putObjectContentMD5Hex(c)
	if md5Err != nil {
		mapError(c, md5Err)
		return
	}
	result, putErr := s.putObjectWithUserMetadataAndMD5(ctx, bucket, key, body, sizeHint, contentType, putObjectACL(c), userMetadata, systemMetadata, contentMD5Hex)
	if putErr != nil {
		mapError(c, putErr)
		return
	}
	obj := result.Object

	if len(tags) > 0 {
		versionID := obj.VersionID
		if err := s.ops.SetObjectTags(bucket, key, versionID, tags); err != nil {
			mapError(c, err)
			return
		}
	}

	responseStart := time.Now()
	c.Header("ETag", fmt.Sprintf("\"%s\"", obj.ETag))
	writeSSEAlgorithmHeader(c, obj.SSEAlgorithm)
	if obj.VersionID != "" {
		c.Header("X-Amz-Version-Id", obj.VersionID)
	}
	c.Status(consts.StatusOK)
	cluster.ObservePutTraceStage(ctx, cluster.PutTraceStageHTTPPutResponse, responseStart, cluster.PutTraceStageFields{})
}

func (s *Server) deleteObject(ctx context.Context, c *app.RequestContext) {
	if s.isDegraded() {
		writeXMLError(c, consts.StatusServiceUnavailable, "ServiceUnavailable", "system is in degraded mode: writes suspended")
		return
	}

	bucket := c.Param("bucket")
	key := getKey(c)
	if key == "" {
		s.deleteBucket(ctx, c)
		return
	}

	if c.QueryArgs().Has("tagging") {
		s.deleteObjectTagging(ctx, c)
		return
	}

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
