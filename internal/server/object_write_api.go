package server

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/cluster"
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

	prepareStart := time.Now()
	systemMetadata, sseErr := parseObjectSSEHeaders(c)
	if sseErr != nil {
		writeSSEHeaderError(c, sseErr)
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
	cluster.ObservePutTraceStage(ctx, cluster.PutTraceStageHTTPPutPrepare, prepareStart, cluster.PutTraceStageFields{
		Bytes: int64(len(rawBody)),
	})

	result, putErr := s.putObjectWithUserMetadata(ctx, bucket, key, body, contentType, putObjectACL(c), userMetadata, systemMetadata)
	if putErr != nil {
		mapError(c, putErr)
		return
	}
	obj := result.Object

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
