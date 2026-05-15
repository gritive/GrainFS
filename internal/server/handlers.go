package server

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/adminapi"
	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/eventstore"
	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/s3auth"
	"github.com/gritive/GrainFS/internal/storage"
)

// S3 XML response types.

type listBucketsResult struct {
	XMLName xml.Name       `xml:"ListAllMyBucketsResult"`
	Xmlns   string         `xml:"xmlns,attr"`
	Buckets []bucketResult `xml:"Buckets>Bucket"`
}

type bucketResult struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

type listObjectsResult struct {
	XMLName     xml.Name       `xml:"ListBucketResult"`
	Xmlns       string         `xml:"xmlns,attr"`
	Name        string         `xml:"Name"`
	Prefix      string         `xml:"Prefix"`
	MaxKeys     int            `xml:"MaxKeys"`
	IsTruncated bool           `xml:"IsTruncated"`
	Contents    []objectResult `xml:"Contents"`
}

type objectResult struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
}

type initiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadId string   `xml:"UploadId"`
}

type completeMultipartUploadRequest struct {
	XMLName xml.Name      `xml:"CompleteMultipartUpload"`
	Parts   []xmlPartInfo `xml:"Part"`
}

type xmlPartInfo struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

type completeMultipartUploadResult struct {
	XMLName xml.Name `xml:"CompleteMultipartUploadResult"`
	Xmlns   string   `xml:"xmlns,attr"`
	Bucket  string   `xml:"Bucket"`
	Key     string   `xml:"Key"`
	ETag    string   `xml:"ETag"`
}

type listMultipartUploadsResult struct {
	XMLName     xml.Name             `xml:"ListMultipartUploadsResult"`
	Xmlns       string               `xml:"xmlns,attr"`
	Bucket      string               `xml:"Bucket"`
	Prefix      string               `xml:"Prefix,omitempty"`
	MaxUploads  int                  `xml:"MaxUploads"`
	IsTruncated bool                 `xml:"IsTruncated"`
	Uploads     []multipartUploadXML `xml:"Upload"`
}

type multipartUploadXML struct {
	Key       string `xml:"Key"`
	UploadId  string `xml:"UploadId"`
	Initiated string `xml:"Initiated"`
}

type listPartsResult struct {
	XMLName     xml.Name      `xml:"ListPartsResult"`
	Xmlns       string        `xml:"xmlns,attr"`
	Bucket      string        `xml:"Bucket"`
	Key         string        `xml:"Key"`
	UploadId    string        `xml:"UploadId"`
	MaxParts    int           `xml:"MaxParts"`
	IsTruncated bool          `xml:"IsTruncated"`
	Parts       []partInfoXML `xml:"Part"`
}

type partInfoXML struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
	Size       int64  `xml:"Size"`
}

type s3Error struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

const (
	readAfterWriteRetryTimeout  = 500 * time.Millisecond
	readAfterWriteRetryInterval = 10 * time.Millisecond
)

func writeXMLError(c *app.RequestContext, status int, code, message string) {
	c.SetContentType("application/xml")
	data, _ := xml.Marshal(s3Error{Code: code, Message: message})
	c.Data(status, "application/xml", data)
}

func mapError(c *app.RequestContext, err error) {
	switch {
	case errors.Is(err, storage.ErrPreconditionFailed):
		writeXMLError(c, consts.StatusPreconditionFailed, "PreconditionFailed", err.Error())
	case errors.Is(err, storage.ErrInvalidCopySource):
		writeXMLError(c, consts.StatusBadRequest, "InvalidArgument", err.Error())
	case errors.Is(err, storage.ErrBucketNotFound):
		writeXMLError(c, consts.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist")
	case errors.Is(err, storage.ErrBucketAlreadyExists):
		writeXMLError(c, consts.StatusConflict, "BucketAlreadyOwnedByYou", "Your previous request to create the named bucket succeeded")
	case errors.Is(err, storage.ErrBucketNotEmpty):
		writeXMLError(c, consts.StatusConflict, "BucketNotEmpty", "The bucket you tried to delete is not empty")
	case errors.Is(err, storage.ErrObjectNotFound):
		writeXMLError(c, consts.StatusNotFound, "NoSuchKey", "The specified key does not exist")
	case errors.Is(err, storage.ErrUploadNotFound):
		writeXMLError(c, consts.StatusNotFound, "NoSuchUpload", "The specified upload does not exist")
	case errors.Is(err, storage.ErrEntityTooLarge):
		writeXMLError(c, consts.StatusRequestEntityTooLarge, "EntityTooLarge", "Your proposed upload exceeds the maximum allowed object size")
	case errors.Is(err, storage.ErrForwardBackpressure):
		writeXMLError(c, consts.StatusServiceUnavailable, "SlowDown", "too many forwarded upload streams in flight")
	case errors.Is(err, cluster.ErrPlacementTargetsUnavailable):
		writeXMLError(c, consts.StatusServiceUnavailable, "ServiceUnavailable", err.Error())
	case errors.Is(err, storage.ErrUnsupportedOperation):
		writeXMLError(c, consts.StatusNotImplemented, "NotImplemented", err.Error())
	default:
		writeXMLError(c, consts.StatusInternalServerError, "InternalError", err.Error())
	}
}

func getKey(c *app.RequestContext) string {
	key := c.Param("key")
	return strings.TrimPrefix(key, "/")
}

func (s *Server) listBuckets(ctx context.Context, c *app.RequestContext) {
	buckets, err := s.ops.ListBuckets(ctx)
	if err != nil {
		mapError(c, err)
		return
	}

	// CR1: filter by key's BucketScope so a scoped key cannot enumerate
	// bucket names outside its scope. scope=nil means unrestricted (legacy keys).
	if scope := iam.ScopeFromContext(ctx); len(scope) > 0 {
		filtered := buckets[:0]
		for _, name := range buckets {
			if iam.ScopeAllows(scope, name) {
				filtered = append(filtered, name)
			}
		}
		buckets = filtered
	}

	// Filter internal GrainFS buckets — they are never user-facing S3 buckets.
	{
		filtered := buckets[:0]
		for _, name := range buckets {
			if !storage.IsInternalBucket(name) {
				filtered = append(filtered, name)
			}
		}
		buckets = filtered
	}

	result := listBucketsResult{Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/"}
	for _, name := range buckets {
		result.Buckets = append(result.Buckets, bucketResult{
			Name:         name,
			CreationDate: time.Now().UTC().Format(time.RFC3339),
		})
	}

	data, _ := xml.Marshal(result)
	c.Data(consts.StatusOK, "application/xml", data)
}

func (s *Server) createBucket(ctx context.Context, c *app.RequestContext) {
	bucket := c.Param("bucket")

	// PUT /:bucket?policy — set bucket policy
	if c.QueryArgs().Has("policy") {
		s.putBucketPolicy(c, bucket)
		return
	}

	// PUT /:bucket?lifecycle — set bucket lifecycle configuration
	if c.QueryArgs().Has("lifecycle") {
		s.putBucketLifecycle(ctx, c, bucket)
		return
	}

	// PUT /:bucket?versioning — set versioning state
	if c.QueryArgs().Has("versioning") {
		s.putBucketVersioning(c, bucket)
		return
	}

	if err := s.ops.CreateBucket(ctx, bucket); err != nil {
		mapError(c, err)
		return
	}
	metrics.BucketsTotal.Inc()
	s.issueCreatorGrant(ctx, bucket)
	s.mutations.OnBucketCreate(ctx, bucket)
	c.Header("Location", "/"+bucket)
	c.Status(consts.StatusOK)
}

// issueCreatorGrant issues an explicit Admin grant to the request principal
// on the newly-created bucket (P5). Best-effort: if the proposer is not
// wired (e.g., anonymous mode, pre-bootstrap window) or the propose fails,
// the bucket is still created — the grant can be re-issued via admin API.
func (s *Server) issueCreatorGrant(ctx context.Context, bucket string) {
	if s.iamProposer == nil {
		return
	}
	principal := iam.PrincipalFromContext(ctx)
	if principal == "" {
		return
	}
	g := iam.Grant{
		SAID:      principal,
		Bucket:    bucket,
		Role:      iam.RoleAdmin,
		CreatedAt: time.Now().UTC(),
		CreatedBy: principal,
	}
	if err := s.iamProposer.ProposeGrantPut(ctx, g); err != nil {
		log.Warn().Err(err).Str("sa", principal).Str("bucket", bucket).
			Msg("iam: failed to issue creator grant; bucket created without explicit grant")
	}
}

// unwrapBackend returns the innermost backend, unwrapping decorators like CachedBackend.
type unwrapper interface {
	Unwrap() storage.Backend
}

func unwrapBackend(b storage.Backend) storage.Backend {
	for {
		u, ok := b.(unwrapper)
		if !ok {
			return b
		}
		b = u.Unwrap()
	}
}

func (s *Server) headBucket(ctx context.Context, c *app.RequestContext) {
	bucket := c.Param("bucket")
	if err := s.ops.HeadBucket(ctx, bucket); err != nil {
		mapError(c, err)
		return
	}
	c.Status(consts.StatusOK)
}

func (s *Server) deleteBucket(ctx context.Context, c *app.RequestContext) {
	bucket := c.Param("bucket")

	// DELETE /:bucket?lifecycle — delete bucket lifecycle configuration
	if c.QueryArgs().Has("lifecycle") {
		s.deleteBucketLifecycle(ctx, c, bucket)
		return
	}

	// DELETE /:bucket?policy — delete bucket policy
	if c.QueryArgs().Has("policy") {
		s.deleteBucketPolicy(c, bucket)
		return
	}

	if err := s.ops.DeleteBucket(ctx, bucket); err != nil {
		mapError(c, err)
		return
	}
	metrics.BucketsTotal.Dec()
	s.mutations.OnBucketDelete(ctx, bucket)
	c.Status(consts.StatusNoContent)
}

func (s *Server) listObjects(ctx context.Context, c *app.RequestContext) {
	bucket := c.Param("bucket")

	// GET /:bucket?lifecycle — get bucket lifecycle configuration
	if c.QueryArgs().Has("lifecycle") {
		s.getBucketLifecycle(ctx, c, bucket)
		return
	}

	// GET /:bucket?policy — get bucket policy
	if c.QueryArgs().Has("policy") {
		s.getBucketPolicy(c, bucket)
		return
	}

	// GET /:bucket?versioning — get versioning state
	if c.QueryArgs().Has("versioning") {
		s.getBucketVersioning(ctx, c, bucket)
		return
	}

	// GET /:bucket?versions — list all object versions
	if c.QueryArgs().Has("versions") {
		s.listObjectVersions(ctx, c, bucket)
		return
	}

	// GET /:bucket?uploads — list in-progress multipart uploads
	if c.QueryArgs().Has("uploads") {
		s.listMultipartUploads(ctx, c, bucket)
		return
	}

	prefix := string(c.QueryArgs().Peek("prefix"))
	maxKeys := 1000
	if mk := string(c.QueryArgs().Peek("max-keys")); mk != "" {
		if v, err := strconv.Atoi(mk); err == nil && v > 0 {
			maxKeys = v
		}
	}

	objects, err := s.ops.ListObjects(ctx, bucket, prefix, maxKeys)
	if err != nil {
		mapError(c, err)
		return
	}

	result := listObjectsResult{
		Xmlns:   "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:    bucket,
		Prefix:  prefix,
		MaxKeys: maxKeys,
	}
	for _, obj := range objects {
		result.Contents = append(result.Contents, objectResult{
			Key:          obj.Key,
			LastModified: time.Unix(obj.LastModified, 0).UTC().Format(time.RFC3339),
			ETag:         fmt.Sprintf("\"%s\"", obj.ETag),
			Size:         obj.Size,
		})
	}

	data, _ := xml.Marshal(result)
	c.Data(consts.StatusOK, "application/xml", data)
}

func (s *Server) handlePut(ctx context.Context, c *app.RequestContext) {
	requestStart := time.Now()
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
	contentType := string(c.GetHeader("Content-Type"))
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	rawBody := c.Request.Body()

	// Handle aws-chunked Content-Encoding (used by AWS SDKs for streaming uploads)
	contentEncoding := string(c.GetHeader("Content-Encoding"))
	contentSHA := string(c.GetHeader("X-Amz-Content-Sha256"))
	isAWSChunked := contentEncoding == "aws-chunked" || contentSHA == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	if isAWSChunked {
		decoded, err := s3auth.DecodeAWSChunkedBody(rawBody)
		if err != nil {
			c.AbortWithMsg(fmt.Sprintf("invalid aws-chunked encoding: %v", err), 400)
			return
		}
		rawBody = decoded
	} else if expected := c.Request.Header.ContentLength(); expected >= 0 && expected != len(rawBody) {
		c.AbortWithMsg(
			fmt.Sprintf("request body size mismatch: content-length=%d actual=%d", expected, len(rawBody)),
			400,
		)
		return
	}

	body := bytes.NewReader(rawBody)
	aclHeader := string(c.GetHeader("x-amz-acl"))
	userMetadata := copyUserMetadata(c)
	cluster.ObservePutTraceStage(ctx, cluster.PutTraceStageHTTPPutPrepare, prepareStart, cluster.PutTraceStageFields{
		Bytes: int64(len(rawBody)),
	})

	var (
		result *storage.PutObjectResult
		putErr error
	)
	backendStart := time.Now()
	if aclHeader != "" {
		acl := s3auth.ParseACLHeader(aclHeader)
		result, putErr = s.ops.PutObjectWithACLAndUserMetadataResult(ctx, bucket, key, body, contentType, uint8(acl), userMetadata)
	} else {
		result, putErr = s.ops.PutObjectWithUserMetadataResult(ctx, bucket, key, body, contentType, userMetadata)
	}
	cluster.ObservePutTraceStage(ctx, cluster.PutTraceStageHTTPPutBackend, backendStart, cluster.PutTraceStageFields{})
	if putErr != nil {
		mapError(c, putErr)
		return
	}
	obj := result.Object

	mutationStart := time.Now()
	s.mutations.OnObjectWrite(ctx, bucket, key, result)
	cluster.ObservePutTraceStage(ctx, cluster.PutTraceStageHTTPPutMutation, mutationStart, cluster.PutTraceStageFields{})

	responseStart := time.Now()
	c.Header("ETag", fmt.Sprintf("\"%s\"", obj.ETag))
	if obj.VersionID != "" {
		c.Header("X-Amz-Version-Id", obj.VersionID)
	}
	c.Status(consts.StatusOK)
	cluster.ObservePutTraceStage(ctx, cluster.PutTraceStageHTTPPutResponse, responseStart, cluster.PutTraceStageFields{})
}

type objectMetricDelta struct {
	objects int64
	bytes   int64
}

func objectWriteMetricDelta(previous storage.PreviousObject, newSize int64) objectMetricDelta {
	if previous.Exists {
		return objectMetricDelta{bytes: -previous.Size + newSize}
	}
	return objectMetricDelta{objects: 1, bytes: newSize}
}

func objectDeleteMetricDelta(previous storage.PreviousObject) objectMetricDelta {
	if !previous.Exists {
		return objectMetricDelta{}
	}
	return objectMetricDelta{objects: -1, bytes: -previous.Size}
}

func applyObjectMetricDelta(delta objectMetricDelta) {
	if delta.objects != 0 {
		metrics.ObjectsTotal.Add(float64(delta.objects))
	}
	if delta.bytes != 0 {
		metrics.StorageBytesTotal.Add(float64(delta.bytes))
	}
}

func recordObjectWriteMetrics(previous storage.PreviousObject, newSize int64) {
	applyObjectMetricDelta(objectWriteMetricDelta(previous, newSize))
}

func recordObjectDeleteMetrics(previous storage.PreviousObject) {
	applyObjectMetricDelta(objectDeleteMetricDelta(previous))
}

func (s *Server) getObjectWithReadAfterWriteRetry(ctx context.Context, bucket, key string) (io.ReadCloser, *storage.Object, error) {
	deadline := time.Now().Add(readAfterWriteRetryTimeout)
	var lastErr error
	for {
		rc, obj, err := s.ops.GetObject(ctx, bucket, key)
		if !errors.Is(err, storage.ErrObjectNotFound) {
			return rc, obj, err
		}
		lastErr = err
		if time.Now().After(deadline) {
			return nil, nil, lastErr
		}
		if !sleepReadAfterWriteRetry(ctx) {
			return nil, nil, ctx.Err()
		}
	}
}

func (s *Server) headObjectWithReadAfterWriteRetry(ctx context.Context, bucket, key string) (*storage.Object, error) {
	deadline := time.Now().Add(readAfterWriteRetryTimeout)
	var lastErr error
	for {
		obj, err := s.ops.HeadObject(ctx, bucket, key)
		if !errors.Is(err, storage.ErrObjectNotFound) {
			return obj, err
		}
		lastErr = err
		if time.Now().After(deadline) {
			return nil, lastErr
		}
		if !sleepReadAfterWriteRetry(ctx) {
			return nil, ctx.Err()
		}
	}
}

func sleepReadAfterWriteRetry(ctx context.Context) bool {
	timer := time.NewTimer(readAfterWriteRetryInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (s *Server) getObject(ctx context.Context, c *app.RequestContext) {
	if ri := s.readIndexer; ri != nil {
		readIdx, err := ri.ReadIndex(ctx)
		if err == nil {
			err = ri.WaitApplied(ctx, readIdx)
		}
		if err != nil {
			mapError(c, err)
			return
		}
	}

	bucket := c.Param("bucket")
	key := getKey(c)

	// GET /:bucket/:key?uploadId=<id> — list parts for one in-progress multipart.
	// Checked before versionId / Range because S3 routes the request to ListParts
	// whenever uploadId is present, even when other query strings appear.
	if uploadID := string(c.QueryArgs().Peek("uploadId")); uploadID != "" {
		s.listParts(ctx, c, bucket, key, uploadID)
		return
	}

	versionID := string(c.QueryArgs().Peek("versionId"))
	rangeHeader := string(c.GetHeader("Range"))
	if versionID == "" && rangeHeader != "" {
		if s.getObjectRangeReadAt(ctx, c, bucket, key, rangeHeader) {
			return
		}
	}

	var rc io.ReadCloser
	var obj *storage.Object
	var err error

	if versionID != "" {
		rc, obj, err = s.ops.GetObjectVersion(bucket, key, versionID)
	} else {
		rc, obj, err = s.getObjectWithReadAfterWriteRetry(ctx, bucket, key)
	}
	if err != nil {
		if errors.Is(err, storage.ErrMethodNotAllowed) {
			c.Header("x-amz-delete-marker", "true")
			if versionID != "" {
				c.Header("x-amz-version-id", versionID)
			}
			writeXMLError(c, consts.StatusMethodNotAllowed, "MethodNotAllowed", "The specified method is not allowed against this resource.")
			return
		}
		mapError(c, err)
		return
	}
	defer func() {
		if rc != nil {
			rc.Close()
		}
	}()

	if s.mustAuthorizePostLoad(ctx, c, bucket, key, s3auth.GetObject, obj.ACL) {
		return
	}

	s.emitEvent(eventstore.Event{Type: eventstore.EventTypeS3, Action: eventstore.EventActionGet, Bucket: bucket, Key: key, Size: obj.Size})
	etag := fmt.Sprintf("\"%s\"", obj.ETag)
	c.Header("Content-Type", obj.ContentType)
	c.Header("ETag", etag)
	c.Header("Last-Modified", time.Unix(obj.LastModified, 0).UTC().Format(http.TimeFormat))
	c.Header("Accept-Ranges", "bytes")
	writeUserMetadataHeaders(c, obj.UserMetadata)
	if obj.VersionID != "" {
		c.Header("X-Amz-Version-Id", obj.VersionID)
	}
	if s.verifier != nil {
		c.Header("Cache-Control", "private, no-store")
	} else {
		c.Header("Cache-Control", "public, max-age=3600")
	}

	if !checkConditionals(c, etag, obj.LastModified) {
		return
	}

	// Handle Range requests: must use standard path (sendfile transfers entire file)
	if rangeHeader != "" {
		start, end, ok := parseByteRange(rangeHeader, obj.Size)
		if !ok {
			c.Status(consts.StatusRequestedRangeNotSatisfiable)
			c.Header("Content-Range", fmt.Sprintf("bytes */%d", obj.Size))
			return
		}

		// Seek to start, read requested range
		if seeker, ok := rc.(io.Seeker); ok {
			if _, err := seeker.Seek(start, io.SeekStart); err != nil {
				mapError(c, err)
				return
			}
		} else {
			if _, err := io.CopyN(io.Discard, rc, start); err != nil {
				mapError(c, err)
				return
			}
		}

		length := end - start + 1
		c.Header("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, obj.Size))
		c.Header("Content-Length", strconv.FormatInt(length, 10))
		c.Set(auditBytesOutKey, length)
		c.Response.SetBodyStream(io.NopCloser(io.LimitReader(rc, length)), int(length))
		c.Status(consts.StatusPartialContent)
		rc = nil
		return
	}

	// Zero-copy for large non-range requests
	c.Set(auditBytesOutKey, obj.Size)
	if obj.Size > 16*1024 {
		c.Response.SetBodyStream(rc, int(obj.Size))
		c.Status(consts.StatusOK)
		rc = nil
	} else {
		c.Header("Content-Length", strconv.FormatInt(obj.Size, 10))
		data, err := io.ReadAll(rc)
		if err != nil {
			mapError(c, err)
			return
		}
		c.Data(consts.StatusOK, obj.ContentType, data)
	}
}

type objectReadAtBackend interface {
	ReadAt(ctx context.Context, bucket, key string, offset int64, buf []byte) (int, error)
}

const maxRangeReadAtChunk = 1 << 20

var readAtRangeBufferPool = sync.Pool{
	New: func() any {
		buf := make([]byte, maxRangeReadAtChunk)
		return &buf
	},
}

type readAtRangeReader struct {
	ctx            context.Context
	backend        objectReadAtBackend
	bucket, key    string
	offset, length int64
	pos            int64
	buf            []byte
	bufPos         int
	bufEnd         int
	pooled         bool
}

func (r *readAtRangeReader) Read(p []byte) (int, error) {
	if r.pos >= r.length {
		return 0, io.EOF
	}
	if r.bufPos >= r.bufEnd {
		if r.buf == nil {
			size := r.length
			if size > maxRangeReadAtChunk {
				size = maxRangeReadAtChunk
			}
			if size == maxRangeReadAtChunk {
				bufp := readAtRangeBufferPool.Get().(*[]byte)
				r.buf = (*bufp)[:maxRangeReadAtChunk]
				r.pooled = true
			} else {
				r.buf = make([]byte, int(size))
			}
		}
		want := len(r.buf)
		if remaining := r.length - r.pos; int64(want) > remaining {
			want = int(remaining)
		}
		n, err := r.backend.ReadAt(r.ctx, r.bucket, r.key, r.offset+r.pos, r.buf[:want])
		if n > 0 {
			r.bufPos = 0
			r.bufEnd = n
		}
		if err != nil && n == 0 {
			return 0, err
		}
		if n == 0 {
			return 0, io.EOF
		}
	}
	n := copy(p, r.buf[r.bufPos:r.bufEnd])
	r.bufPos += n
	r.pos += int64(n)
	return n, nil
}

func (r *readAtRangeReader) Close() error {
	if r.pooled && r.buf != nil {
		buf := r.buf[:maxRangeReadAtChunk]
		readAtRangeBufferPool.Put(&buf)
	}
	r.buf = nil
	r.bufPos = 0
	r.bufEnd = 0
	r.pooled = false
	return nil
}

func (s *Server) getObjectRangeReadAt(ctx context.Context, c *app.RequestContext, bucket, key, rangeHeader string) bool {
	reader, ok := s.backend.(objectReadAtBackend)
	if !ok {
		return false
	}

	obj, err := s.headObjectWithReadAfterWriteRetry(ctx, bucket, key)
	if err != nil {
		mapError(c, err)
		return true
	}

	etag := fmt.Sprintf("\"%s\"", obj.ETag)
	c.Header("Content-Type", obj.ContentType)
	c.Header("ETag", etag)
	c.Header("Last-Modified", time.Unix(obj.LastModified, 0).UTC().Format(http.TimeFormat))
	c.Header("Accept-Ranges", "bytes")
	if obj.VersionID != "" {
		c.Header("X-Amz-Version-Id", obj.VersionID)
	}
	if s.verifier != nil {
		c.Header("Cache-Control", "private, no-store")
	} else {
		c.Header("Cache-Control", "public, max-age=3600")
	}

	if s.mustAuthorizePostLoad(ctx, c, bucket, key, s3auth.GetObject, obj.ACL) {
		return true
	}
	if !checkConditionals(c, etag, obj.LastModified) {
		return true
	}

	start, end, ok := parseByteRange(rangeHeader, obj.Size)
	if !ok {
		c.Status(consts.StatusRequestedRangeNotSatisfiable)
		c.Header("Content-Range", fmt.Sprintf("bytes */%d", obj.Size))
		return true
	}

	length := end - start + 1
	s.emitEvent(eventstore.Event{Type: eventstore.EventTypeS3, Action: eventstore.EventActionGet, Bucket: bucket, Key: key, Size: obj.Size})
	c.Header("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, obj.Size))
	c.Header("Content-Length", strconv.FormatInt(length, 10))
	c.Set(auditBytesOutKey, length)
	c.Response.SetBodyStream(&readAtRangeReader{
		ctx:     ctx,
		backend: reader,
		bucket:  bucket,
		key:     key,
		offset:  start,
		length:  length,
	}, int(length))
	c.Status(consts.StatusPartialContent)
	return true
}

// parseByteRange parses a "bytes=start-end" Range header.
// Returns (start, end, ok). end is inclusive.
func parseByteRange(rangeHeader string, size int64) (int64, int64, bool) {
	if !strings.HasPrefix(rangeHeader, "bytes=") {
		return 0, 0, false
	}
	rangeSpec := strings.TrimPrefix(rangeHeader, "bytes=")
	// Only handle single range (first one)
	parts := strings.SplitN(rangeSpec, ",", 2)
	spec := strings.TrimSpace(parts[0])

	dash := strings.Index(spec, "-")
	if dash < 0 {
		return 0, 0, false
	}

	startStr := spec[:dash]
	endStr := spec[dash+1:]

	var start, end int64
	if startStr == "" {
		// suffix-range: bytes=-N (last N bytes)
		if size == 0 {
			return 0, 0, false
		}
		n, err := strconv.ParseInt(endStr, 10, 64)
		if err != nil || n <= 0 {
			return 0, 0, false
		}
		if n > size {
			n = size
		}
		start = size - n
		end = size - 1
	} else {
		var err error
		start, err = strconv.ParseInt(startStr, 10, 64)
		if err != nil || start < 0 || start >= size {
			return 0, 0, false
		}
		if endStr == "" {
			end = size - 1
		} else {
			end, err = strconv.ParseInt(endStr, 10, 64)
			if err != nil || end < start {
				return 0, 0, false
			}
			if end >= size {
				end = size - 1
			}
		}
	}
	return start, end, true
}

func (s *Server) headObject(ctx context.Context, c *app.RequestContext) {
	if ri := s.readIndexer; ri != nil {
		readIdx, err := ri.ReadIndex(ctx)
		if err == nil {
			err = ri.WaitApplied(ctx, readIdx)
		}
		if err != nil {
			mapError(c, err)
			return
		}
	}

	bucket := c.Param("bucket")
	key := getKey(c)

	versionID := string(c.QueryArgs().Peek("versionId"))
	var obj *storage.Object
	var err error
	if versionID != "" {
		obj, err = s.ops.HeadObjectVersion(bucket, key, versionID)
	} else {
		obj, err = s.headObjectWithReadAfterWriteRetry(ctx, bucket, key)
	}
	if err != nil {
		if errors.Is(err, storage.ErrMethodNotAllowed) {
			c.Header("x-amz-delete-marker", "true")
			if versionID != "" {
				c.Header("x-amz-version-id", versionID)
			}
			writeXMLError(c, consts.StatusMethodNotAllowed, "MethodNotAllowed", "The specified method is not allowed against this resource.")
			return
		}
		mapError(c, err)
		return
	}

	if s.mustAuthorizePostLoad(ctx, c, bucket, key, s3auth.HeadObject, obj.ACL) {
		return
	}

	if obj.VersionID != "" {
		c.Header("x-amz-version-id", obj.VersionID)
	}

	etag := fmt.Sprintf("\"%s\"", obj.ETag)
	c.Header("Content-Type", obj.ContentType)
	c.Header("Content-Length", strconv.FormatInt(obj.Size, 10))
	c.Header("ETag", etag)
	c.Header("Last-Modified", time.Unix(obj.LastModified, 0).UTC().Format(http.TimeFormat))
	c.Header("Accept-Ranges", "bytes")
	writeUserMetadataHeaders(c, obj.UserMetadata)
	if s.verifier != nil {
		c.Header("Cache-Control", "private, no-store")
	} else {
		c.Header("Cache-Control", "public, max-age=3600")
	}

	if !checkConditionals(c, etag, obj.LastModified) {
		return
	}

	c.Status(consts.StatusOK)
}

// checkConditionals evaluates RFC 7232 conditional request headers.
// Returns false and sets response status if the request is short-circuited.
func checkConditionals(c *app.RequestContext, etag string, lastModifiedUnix int64) bool {
	if im := string(c.GetHeader("If-Match")); im != "" {
		if im != "*" && im != etag {
			c.Status(consts.StatusPreconditionFailed)
			return false
		}
	}
	if inm := string(c.GetHeader("If-None-Match")); inm != "" {
		if inm == etag || inm == "*" {
			c.Status(consts.StatusNotModified)
			return false
		}
	}
	if ims := string(c.GetHeader("If-Modified-Since")); ims != "" {
		if t, err := http.ParseTime(ims); err == nil {
			if !time.Unix(lastModifiedUnix, 0).After(t) {
				c.Status(consts.StatusNotModified)
				return false
			}
		}
	}
	if ius := string(c.GetHeader("If-Unmodified-Since")); ius != "" {
		if t, err := http.ParseTime(ius); err == nil {
			if time.Unix(lastModifiedUnix, 0).After(t) {
				c.Status(consts.StatusPreconditionFailed)
				return false
			}
		}
	}
	return true
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
		if err := s.ops.AbortMultipartUpload(ctx, bucket, key, uploadID); err != nil {
			mapError(c, err)
			return
		}
		c.Status(consts.StatusNoContent)
		return
	}

	// DELETE /:bucket/:key?versionId=<id> — hard-delete specific version
	if versionID := string(c.QueryArgs().Peek("versionId")); versionID != "" {
		if err := s.ops.DeleteObjectVersion(bucket, key, versionID); err != nil {
			mapError(c, err)
			return
		}
		c.Status(consts.StatusNoContent)
		return
	}

	result, err := s.ops.DeleteObjectWithResult(ctx, bucket, key)
	if err != nil {
		mapError(c, err)
		return
	}
	if result.Deleted.DeleteMarker {
		c.Header("x-amz-delete-marker", "true")
		c.Header("x-amz-version-id", result.Deleted.VersionID)
	}
	s.mutations.OnObjectDelete(ctx, bucket, key, result)
	c.Status(consts.StatusNoContent)
}

// handlePost routes POST requests for multipart upload operations.
func (s *Server) handlePost(ctx context.Context, c *app.RequestContext) {
	if s.isDegraded() {
		writeXMLError(c, consts.StatusServiceUnavailable, "ServiceUnavailable", "system is in degraded mode: writes suspended")
		return
	}

	bucket := c.Param("bucket")
	key := getKey(c)

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

// handleFormUpload processes S3 POST form-based uploads (browser direct upload).
// The form contains: key, Content-Type, policy, X-Amz-Signature, file, etc.
func (s *Server) handleFormUpload(ctx context.Context, c *app.RequestContext, bucket string) {
	form, err := c.MultipartForm()
	if err != nil {
		writeXMLError(c, consts.StatusBadRequest, "MalformedPOSTRequest", "cannot parse multipart form")
		return
	}

	// Extract key from form field
	keys := form.Value["key"]
	if len(keys) == 0 {
		writeXMLError(c, consts.StatusBadRequest, "InvalidArgument", "missing 'key' form field")
		return
	}
	key := keys[0]
	c.Set(auditObjectKeyKey, key)

	// Validate POST policy if authentication is enabled.
	if s.verifier != nil {
		policyB64 := ""
		if ps := form.Value["policy"]; len(ps) > 0 {
			policyB64 = ps[0]
		}
		sig := ""
		if ss := form.Value["X-Amz-Signature"]; len(ss) > 0 {
			sig = ss[0]
		}
		if policyB64 == "" || sig == "" {
			writeXMLError(c, consts.StatusForbidden, "AccessDenied", "missing POST policy signature")
			return
		}

		// Validate expiration
		if err := s3auth.ValidatePostPolicyExpiration(policyB64); err != nil {
			writeXMLError(c, consts.StatusForbidden, "AccessDenied", err.Error())
			return
		}

		// Validate conditions
		formFields := map[string]string{
			"bucket": bucket,
			"key":    key,
		}
		for k, vs := range form.Value {
			if len(vs) > 0 {
				formFields[k] = vs[0]
			}
		}
		if err := s3auth.ValidatePostPolicyConditions(policyB64, formFields); err != nil {
			writeXMLError(c, consts.StatusForbidden, "AccessDenied", err.Error())
			return
		}

		credential := ""
		if cs := form.Value["X-Amz-Credential"]; len(cs) > 0 {
			credential = cs[0]
		}
		// credential format: AKID/20260416/us-east-1/s3/aws4_request
		parts := strings.SplitN(credential, "/", 5)
		if len(parts) != 5 {
			writeXMLError(c, consts.StatusForbidden, "AccessDenied", "invalid credential")
			return
		}
		accessKey := parts[0]
		date := parts[1]
		region := parts[2]
		service := parts[3]
		secretKey := s.verifier.LookupSecret(accessKey)
		if secretKey == "" {
			writeXMLError(c, consts.StatusForbidden, "AccessDenied", "invalid access key")
			return
		}
		if err := s3auth.VerifyPostPolicy(policyB64, sig, secretKey, date, region, service); err != nil {
			writeXMLError(c, consts.StatusForbidden, "SignatureDoesNotMatch", err.Error())
			return
		}
	}

	// Extract content type (optional)
	contentType := "application/octet-stream"
	if cts := form.Value["Content-Type"]; len(cts) > 0 {
		contentType = cts[0]
	}

	// Get file data
	files := form.File["file"]
	if len(files) == 0 {
		writeXMLError(c, consts.StatusBadRequest, "InvalidArgument", "missing 'file' form field")
		return
	}

	file, err := files[0].Open()
	if err != nil {
		writeXMLError(c, consts.StatusInternalServerError, "InternalError", "cannot open uploaded file")
		return
	}
	defer file.Close()

	result, err := s.ops.PutObjectWithResult(ctx, bucket, key, file, contentType)
	if err != nil {
		mapError(c, err)
		return
	}
	obj := result.Object

	s.mutations.OnObjectWrite(ctx, bucket, key, result)
	if obj.VersionID != "" {
		c.Header("X-Amz-Version-Id", obj.VersionID)
	}

	// Respond with 204 or redirect if success_action_redirect is set
	if redirectURL := form.Value["success_action_redirect"]; len(redirectURL) > 0 && redirectURL[0] != "" {
		u, err := url.Parse(redirectURL[0])
		if err == nil {
			q := u.Query()
			q.Set("bucket", bucket)
			q.Set("key", key)
			q.Set("etag", obj.ETag)
			u.RawQuery = q.Encode()
			c.Redirect(consts.StatusSeeOther, []byte(u.String()))
			return
		}
	}

	statusStr := "204"
	if ss := form.Value["success_action_status"]; len(ss) > 0 {
		statusStr = ss[0]
	}

	switch statusStr {
	case "200":
		c.Status(consts.StatusOK)
	case "201":
		result := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<PostResponse>
  <Location>%s/%s/%s</Location>
  <Bucket>%s</Bucket>
  <Key>%s</Key>
  <ETag>"%s"</ETag>
</PostResponse>`, "", bucket, key, bucket, key, obj.ETag)
		c.Data(consts.StatusCreated, "application/xml", []byte(result))
	default:
		c.Status(consts.StatusNoContent)
	}
}

// listMultipartUploads renders the in-progress multipart uploads for a bucket
// as the S3 ListMultipartUploadsResult XML payload. Honors ?prefix= and
// ?max-uploads= (default 1000). IsTruncated is set conservatively when the
// returned count equals max-uploads — consumers needing exact pagination
// should re-request with smaller max-uploads or wait for follow-up paging
// support. Returns NoSuchBucket when the bucket does not exist.
func (s *Server) listMultipartUploads(ctx context.Context, c *app.RequestContext, bucket string) {
	prefix := string(c.QueryArgs().Peek("prefix"))
	maxUploads := 1000
	if mu := string(c.QueryArgs().Peek("max-uploads")); mu != "" {
		if v, err := strconv.Atoi(mu); err == nil && v > 0 {
			maxUploads = v
		}
	}

	uploads, err := s.ops.ListMultipartUploads(ctx, bucket, prefix, maxUploads)
	if err != nil {
		mapError(c, err)
		return
	}

	result := listMultipartUploadsResult{
		Xmlns:       "http://s3.amazonaws.com/doc/2006-03-01/",
		Bucket:      bucket,
		Prefix:      prefix,
		MaxUploads:  maxUploads,
		IsTruncated: len(uploads) >= maxUploads,
	}
	for _, up := range uploads {
		result.Uploads = append(result.Uploads, multipartUploadXML{
			Key:       up.Key,
			UploadId:  up.UploadID,
			Initiated: time.Unix(up.CreatedAt, 0).UTC().Format(time.RFC3339),
		})
	}
	data, _ := xml.Marshal(result)
	c.Data(consts.StatusOK, "application/xml", data)
}

// listParts renders the parts uploaded so far for one multipart upload as the
// S3 ListPartsResult XML payload. Honors ?max-parts= (default 1000). Returns
// NoSuchUpload (404) when the uploadID does not match an active upload.
func (s *Server) listParts(ctx context.Context, c *app.RequestContext, bucket, key, uploadID string) {
	maxParts := 1000
	if mp := string(c.QueryArgs().Peek("max-parts")); mp != "" {
		if v, err := strconv.Atoi(mp); err == nil && v > 0 {
			maxParts = v
		}
	}

	parts, err := s.ops.ListParts(ctx, bucket, key, uploadID, maxParts)
	if err != nil {
		mapError(c, err)
		return
	}

	result := listPartsResult{
		Xmlns:       "http://s3.amazonaws.com/doc/2006-03-01/",
		Bucket:      bucket,
		Key:         key,
		UploadId:    uploadID,
		MaxParts:    maxParts,
		IsTruncated: len(parts) >= maxParts,
	}
	for _, p := range parts {
		result.Parts = append(result.Parts, partInfoXML{
			PartNumber: p.PartNumber,
			ETag:       fmt.Sprintf("\"%s\"", p.ETag),
			Size:       p.Size,
		})
	}
	data, _ := xml.Marshal(result)
	c.Data(consts.StatusOK, "application/xml", data)
}

func (s *Server) createMultipartUpload(ctx context.Context, c *app.RequestContext, bucket, key string) {
	contentType := string(c.GetHeader("Content-Type"))
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	upload, err := s.ops.CreateMultipartUpload(ctx, bucket, key, contentType)
	if err != nil {
		mapError(c, err)
		return
	}

	result := initiateMultipartUploadResult{
		Xmlns:    "http://s3.amazonaws.com/doc/2006-03-01/",
		Bucket:   bucket,
		Key:      key,
		UploadId: upload.UploadID,
	}
	data, _ := xml.Marshal(result)
	c.Data(consts.StatusOK, "application/xml", data)
}

func (s *Server) uploadPart(ctx context.Context, c *app.RequestContext, bucket, key, uploadID, partNumberStr string) {
	partNumber, err := strconv.Atoi(partNumberStr)
	if err != nil || partNumber < 1 {
		writeXMLError(c, consts.StatusBadRequest, "InvalidArgument", "invalid part number")
		return
	}

	body := bytes.NewReader(c.Request.Body())
	part, err := s.ops.UploadPart(ctx, bucket, key, uploadID, partNumber, body)
	if err != nil {
		mapError(c, err)
		return
	}

	c.Header("ETag", fmt.Sprintf("\"%s\"", part.ETag))
	c.Status(consts.StatusOK)
}

func (s *Server) completeMultipartUpload(ctx context.Context, c *app.RequestContext, bucket, key, uploadID string) {
	var req completeMultipartUploadRequest
	if err := xml.Unmarshal(c.Request.Body(), &req); err != nil {
		writeXMLError(c, consts.StatusBadRequest, "MalformedXML", "invalid XML body")
		return
	}

	parts := make([]storage.Part, len(req.Parts))
	for i, p := range req.Parts {
		etag := strings.Trim(p.ETag, "\"")
		parts[i] = storage.Part{PartNumber: p.PartNumber, ETag: etag}
	}

	result, err := s.ops.CompleteMultipartUploadWithResult(ctx, bucket, key, uploadID, parts)
	if err != nil {
		mapError(c, err)
		return
	}
	obj := result.Object
	s.mutations.OnObjectWrite(ctx, bucket, key, result)
	if obj.VersionID != "" {
		c.Header("X-Amz-Version-Id", obj.VersionID)
	}

	response := completeMultipartUploadResult{
		Xmlns:  "http://s3.amazonaws.com/doc/2006-03-01/",
		Bucket: bucket,
		Key:    key,
		ETag:   fmt.Sprintf("\"%s\"", obj.ETag),
	}
	data, _ := xml.Marshal(response)
	c.Data(consts.StatusOK, "application/xml", data)
}

func (s *Server) serveDashboard(ctx context.Context, c *app.RequestContext) {
	data, err := uiHTML.ReadFile("ui/index.html")
	if err != nil {
		c.String(consts.StatusInternalServerError, "UI not found")
		return
	}
	c.SetContentType("text/html; charset=utf-8")
	c.SetStatusCode(consts.StatusOK)
	if _, err := c.Write(data); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("write dashboard response")
	}
}

// hertzResponseWriter adapts Hertz RequestContext to http.ResponseWriter for stdlib handlers.
type hertzResponseWriter struct {
	c          *app.RequestContext
	header     http.Header
	statusCode int
	written    bool
}

func newResponseWriter(c *app.RequestContext) *hertzResponseWriter {
	return &hertzResponseWriter{c: c, header: make(http.Header), statusCode: http.StatusOK}
}

func (w *hertzResponseWriter) Header() http.Header {
	return w.header
}

func (w *hertzResponseWriter) Write(data []byte) (int, error) {
	if !w.written {
		// Flush accumulated headers to Hertz response
		for k, vs := range w.header {
			for _, v := range vs {
				w.c.Response.Header.Set(k, v)
			}
		}
		w.c.SetStatusCode(w.statusCode)
		w.written = true
	}
	return w.c.Write(data)
}

func (w *hertzResponseWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	if !w.written {
		for k, vs := range w.header {
			for _, v := range vs {
				w.c.Response.Header.Set(k, v)
			}
		}
		w.c.SetStatusCode(statusCode)
		w.written = true
	}
}

// toHTTPRequest converts Hertz RequestContext to a stdlib http.Request for SigV4 verification.
func toHTTPRequest(c *app.RequestContext) *http.Request {
	u := &url.URL{
		Path:     string(c.URI().Path()),
		RawQuery: string(c.URI().QueryString()),
	}
	r := &http.Request{
		Method: string(c.Method()),
		Host:   string(c.Host()),
		URL:    u,
		Header: make(http.Header),
	}

	c.Request.Header.VisitAll(func(key, value []byte) {
		r.Header.Set(string(key), string(value))
	})
	return r
}

func (s *Server) clusterStatus(ctx context.Context, c *app.RequestContext) {
	status := adminapi.Status{
		Mode:      "local",
		Degraded:  s.degradedFlag.Load(),
		DownNodes: []string{},
	}

	if s.cluster != nil {
		status.Mode = "cluster"
		status.NodeID = s.cluster.NodeID()
		status.State = s.cluster.State()
		status.Term = s.cluster.Term()
		status.LeaderID = s.cluster.LeaderID()

		snap := s.cluster.Snapshot()
		if snap.PeerSnapshot != nil {
			status.PeerSnapshot = cluster.PeerLivenessRowsToWire(snap.PeerSnapshot)
			status.Peers = legacyPeersFromSnapshot(snap.PeerSnapshot)
			status.PeerAddrs = legacyPeerAddrsFromSnapshot(snap.PeerSnapshot)
			status.PeerStates = legacyPeerStatesFromSnapshot(snap.PeerSnapshot)
			status.DownNodes = legacyDownNodesFromSnapshot(snap.PeerSnapshot)
		} else {
			status.Peers = s.cluster.Peers()
			if snap.PeerAddrs != nil {
				status.PeerAddrs = snap.PeerAddrs
			}
			if snap.PeerStates != nil {
				status.PeerStates = snap.PeerStates
			}

			// Compute down nodes: configured peers minus live peers.
			livePeers := s.cluster.LivePeers()
			liveSet := make(map[string]struct{}, len(livePeers))
			for _, p := range livePeers {
				liveSet[p] = struct{}{}
			}
			downNodes := []string{}
			for _, p := range s.cluster.Peers() {
				if _, ok := liveSet[p]; !ok {
					downNodes = append(downNodes, p)
				}
			}
			status.DownNodes = downNodes
		}
		if snap.BucketAssignments != nil {
			status.BucketAssignments = snap.BucketAssignments
		}
		if snap.ShardGroups != nil {
			status.ShardGroups = clusterStatusShardGroups(snap.ShardGroups)
		}
		summary := s.cluster.ObjectIndexSummary(string(c.QueryArgs().Peek("bucket")))
		status.ObjectIndexSummary = &adminapi.ObjectIndexSummary{
			Bucket:               summary.Bucket,
			PlacementGroupCounts: summary.PlacementGroupCounts,
		}
	}

	data, _ := json.Marshal(status)
	c.Data(consts.StatusOK, "application/json", data)
}

func (s *Server) clusterPlacement(ctx context.Context, c *app.RequestContext) {
	bucket := string(c.QueryArgs().Peek("bucket"))
	key := string(c.QueryArgs().Peek("key"))
	limit := 100
	if raw := string(c.QueryArgs().Peek("limit")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			c.JSON(consts.StatusBadRequest, map[string]string{"error": "limit must be a positive integer"})
			return
		}
		limit = n
	}
	if key != "" && bucket == "" {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "key filter requires bucket"})
		return
	}
	empty := cluster.PlacementReport{
		DesiredPolicyBasis:  "group_voter_count",
		Bucket:              bucket,
		Key:                 key,
		ActualProfileCounts: map[string]int{},
	}
	if s.cluster == nil {
		c.JSON(consts.StatusOK, empty)
		return
	}
	c.JSON(consts.StatusOK, s.cluster.PlacementReport(bucket, key, limit))
}

func clusterStatusShardGroups(groups []cluster.ShardGroupEntry) []adminapi.ShardGroup {
	out := make([]adminapi.ShardGroup, 0, len(groups))
	for _, group := range groups {
		peers := append([]string(nil), group.PeerIDs...)
		out = append(out, adminapi.ShardGroup{ID: group.ID, PeerIDs: peers})
	}
	return out
}

func legacyPeersFromSnapshot(rows []cluster.PeerLivenessRow) []string {
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		if row.IdentityState == cluster.PeerIdentitySelf {
			continue
		}
		out = append(out, row.PeerID)
	}
	return out
}

func legacyPeerAddrsFromSnapshot(rows []cluster.PeerLivenessRow) map[string]string {
	out := make(map[string]string)
	for _, row := range rows {
		if row.IdentityState == cluster.PeerIdentitySelf || row.RaftAddr == "" {
			continue
		}
		out[row.PeerID] = row.RaftAddr
	}
	return out
}

func legacyPeerStatesFromSnapshot(rows []cluster.PeerLivenessRow) map[string]string {
	out := make(map[string]string)
	for _, row := range rows {
		if row.IdentityState == cluster.PeerIdentitySelf {
			continue
		}
		if row.IdentityState == cluster.PeerIdentityUnresolvedLegacy {
			out[row.PeerID] = string(cluster.PeerIdentityUnresolvedLegacy)
			continue
		}
		out[row.PeerID] = string(row.LivenessState)
	}
	return out
}

func legacyDownNodesFromSnapshot(rows []cluster.PeerLivenessRow) []string {
	out := make([]string, 0)
	for _, row := range rows {
		if row.IdentityState == cluster.PeerIdentitySelf || row.IdentityState == cluster.PeerIdentityUnresolvedLegacy {
			continue
		}
		if cluster.IsExplicitlyDown(row) {
			out = append(out, row.PeerID)
		}
	}
	return out
}

// cacheStatus handles GET /api/cache/status. Reports the volume block
// cache hit/miss counters + resident bytes so the operations dashboard
// can show whether the cache is doing useful work and how full it is.
// CachedBackend (object-level) and readamp (simulator) counters live in
// /metrics; this endpoint is the human-readable summary the dashboard
// consumes.
func (s *Server) cacheStatus(ctx context.Context, c *app.RequestContext) {
	resp := map[string]any{
		"block_cache": map[string]any{
			"enabled": false,
		},
		"shard_cache": map[string]any{
			"enabled": false,
		},
	}
	if s.blockCache != nil {
		stats := s.blockCache.Stats()
		hitRate := 0.0
		if stats.Hits+stats.Misses > 0 {
			hitRate = 100 * float64(stats.Hits) / float64(stats.Hits+stats.Misses)
		}
		resp["block_cache"] = map[string]any{
			"enabled":        stats.CapacityByte > 0,
			"hits":           stats.Hits,
			"misses":         stats.Misses,
			"evictions":      stats.Evictions,
			"resident_bytes": stats.ResidentByte,
			"capacity_bytes": stats.CapacityByte,
			"hit_rate_pct":   hitRate,
		}
	}
	if s.shardCache != nil {
		stats := s.shardCache.Stats()
		hitRate := 0.0
		if stats.Hits+stats.Misses > 0 {
			hitRate = 100 * float64(stats.Hits) / float64(stats.Hits+stats.Misses)
		}
		resp["shard_cache"] = map[string]any{
			"enabled":        stats.CapacityByte > 0,
			"hits":           stats.Hits,
			"misses":         stats.Misses,
			"evictions":      stats.Evictions,
			"resident_bytes": stats.ResidentByte,
			"capacity_bytes": stats.CapacityByte,
			"hit_rate_pct":   hitRate,
		}
	}
	data, _ := json.Marshal(resp)
	c.Data(consts.StatusOK, "application/json", data)
}

// joinClusterHandler handles POST /api/cluster/join for runtime local→cluster transition.
func (s *Server) joinClusterHandler(ctx context.Context, c *app.RequestContext) {
	if resp, blocked := s.mutationGate.BlockResponse("cluster_join"); blocked {
		body, _ := json.Marshal(resp)
		c.Data(resp.Status, "application/json", body)
		return
	}
	if s.joinCluster == nil {
		writeXMLError(c, consts.StatusConflict, "InvalidRequest", "server is already in cluster mode or join not supported")
		return
	}

	var req struct {
		NodeID     string `json:"node_id"`
		RaftAddr   string `json:"raft_addr"`
		Peers      string `json:"peers"`
		ClusterKey string `json:"cluster_key"`
	}
	body, _ := c.Body()
	if err := json.Unmarshal(body, &req); err != nil {
		writeXMLError(c, consts.StatusBadRequest, "MalformedJSON", err.Error())
		return
	}

	if req.RaftAddr == "" || req.Peers == "" {
		writeXMLError(c, consts.StatusBadRequest, "InvalidArgument", "raft_addr and peers are required")
		return
	}

	if err := s.joinCluster(req.NodeID, req.RaftAddr, req.Peers, req.ClusterKey); err != nil {
		resp, _ := json.Marshal(map[string]string{"error": err.Error()})
		c.Data(consts.StatusInternalServerError, "application/json", resp)
		return
	}

	// Clear the join callback so it can't be called twice
	s.joinCluster = nil
	s.emitEvent(eventstore.Event{Type: eventstore.EventTypeSystem, Action: eventstore.EventActionClusterJoin})

	resp, _ := json.Marshal(map[string]string{"status": "joined", "mode": "cluster"})
	c.Data(consts.StatusOK, "application/json", resp)
}

// removePeerHandler handles POST /api/cluster/remove-peer. Removes the named
// voter from the Raft configuration via joint consensus. Pre-flight refuses
// the request when removal would drop live voters below the post-removal
// quorum, unless force=true. Errors:
//   - 400 malformed body or empty id
//   - 404 id is not a current voter
//   - 409 receiver is not the leader (response carries leader_id) or pre-flight blocks
//   - 503 membership controller unwired or mutation gate engaged
//   - 500 engine returned an error
func (s *Server) removePeerHandler(ctx context.Context, c *app.RequestContext) {
	if s.blockIfMutationDisabled(c, "cluster_remove_peer") {
		return
	}
	if s.membership == nil {
		c.JSON(consts.StatusServiceUnavailable, map[string]string{
			"error": "membership controller not configured",
		})
		return
	}
	if s.cluster == nil {
		c.JSON(consts.StatusServiceUnavailable, map[string]string{
			"error": "node is not in cluster mode",
		})
		return
	}

	var req struct {
		ID    string `json:"id"`
		Force bool   `json:"force"`
	}
	body, _ := c.Body()
	if err := json.Unmarshal(body, &req); err != nil {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "malformed JSON body"})
		return
	}
	if req.ID == "" {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "id is required"})
		return
	}

	if s.cluster.State() != "Leader" {
		c.JSON(consts.StatusConflict, map[string]any{
			"error":     "not leader",
			"leader_id": s.cluster.LeaderID(),
		})
		return
	}

	if result, ok := s.evaluateRemovePeerPreflight(req.ID); ok {
		if !result.Allowed {
			if result.Reason != cluster.RemovePeerPreflightQuorumWouldBreak || !req.Force {
				writeRemovePeerPreflightFailure(c, result, req.ID)
				return
			}
		}
	} else {
		writeRemovePeerSnapshotUnavailable(c)
		return
	}

	if err := s.membership.RemoveVoter(ctx, req.ID); err != nil {
		// Leadership can change between the pre-flight check and the engine
		// call. Surface that as 409 + leader_id so the CLI sees the same
		// shape as the up-front "not leader" check and can redirect.
		if errors.Is(err, raft.ErrNotLeader) {
			c.JSON(consts.StatusConflict, map[string]any{
				"error":     "not leader",
				"leader_id": s.cluster.LeaderID(),
			})
			return
		}
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	s.emitEvent(eventstore.Event{
		Type:   eventstore.EventTypeSystem,
		Action: eventstore.EventActionClusterRemovePeer,
		Metadata: map[string]any{
			"removed_id": req.ID,
			"force":      req.Force,
		},
	})

	c.JSON(consts.StatusOK, map[string]string{"status": "removed", "id": req.ID})
}

func (s *Server) evaluateRemovePeerPreflight(id string) (cluster.RemovePeerPreflightResult, bool) {
	snapshot := s.cluster.Snapshot().PeerSnapshot
	if len(snapshot) == 0 {
		return cluster.RemovePeerPreflightResult{}, false
	}
	return cluster.EvaluateRemovePeerPreflight(cluster.RemovePeerPreflightInput{
		TargetID: id,
		Voters:   removePeerPreflightVoters(s.cluster.Peers(), snapshot),
		Snapshot: snapshot,
	}), true
}

func removePeerPreflightVoters(voters []string, snapshot []cluster.PeerLivenessRow) []string {
	if len(voters) == 0 || len(snapshot) == 0 {
		return voters
	}
	byAddr := make(map[string]string, len(snapshot))
	byID := make(map[string]string, len(snapshot))
	for _, row := range snapshot {
		if row.IdentityState == cluster.PeerIdentitySelf || row.PeerID == "" {
			continue
		}
		byID[row.PeerID] = row.PeerID
		if row.RaftAddr != "" {
			byAddr[row.RaftAddr] = row.PeerID
		}
	}
	out := make([]string, len(voters))
	for i, voter := range voters {
		switch {
		case byID[voter] != "":
			out[i] = byID[voter]
		case byAddr[voter] != "":
			out[i] = byAddr[voter]
		default:
			out[i] = voter
		}
	}
	return out
}

func writeRemovePeerSnapshotUnavailable(c *app.RequestContext) {
	c.JSON(consts.StatusConflict, map[string]string{
		"error": "peer snapshot unavailable",
		"hint":  "cluster liveness snapshot is required before membership mutation",
	})
}

func writeRemovePeerPreflightFailure(c *app.RequestContext, result cluster.RemovePeerPreflightResult, id string) {
	switch result.Reason {
	case cluster.RemovePeerPreflightNotInCluster:
		c.JSON(consts.StatusNotFound, map[string]any{
			"error": "peer not in cluster",
			"id":    id,
		})
	case cluster.RemovePeerPreflightIdentityUnresolved:
		c.JSON(consts.StatusConflict, map[string]any{
			"error":          "membership identity unresolved",
			"blocking_peers": result.BlockingPeers,
			"hint":           "remove the unresolved legacy peer first or restore its node ID mapping",
		})
	default:
		c.JSON(consts.StatusConflict, map[string]any{
			"error":        "quorum would break",
			"voters_after": result.VotersAfter,
			"alive_after":  result.AliveAfter,
			"new_quorum":   result.NewQuorum,
			"hint":         "rerun with force=true to override",
		})
	}
}

// handleCopyObject processes PUT with x-amz-copy-source header (S3 CopyObject).
func (s *Server) handleCopyObject(ctx context.Context, c *app.RequestContext, dstBucket, dstKey, copySource string) {
	src, ok := parseCopySource(copySource)
	if !ok {
		writeXMLError(c, consts.StatusBadRequest, "InvalidArgument", "invalid x-amz-copy-source format")
		return
	}

	// Source GetObject authorization. Middleware already gated the destination
	// at PhasePreLoad (PUT to dst). The source bucket needs its own
	// authorization chain — pre-load (IAM + bucket policy) before we touch
	// storage so an unauthorized caller cannot probe object existence via
	// HeadObject, then post-load (ACL) after the source object is loaded.
	if s.mustAuthorize(ctx, c, src.Bucket, src.Key, s3auth.GetObject) {
		return
	}
	srcObj, srcErr := s.ops.HeadObject(ctx, src.Bucket, src.Key)
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
	result, err := s.ops.CopyObject(ctx, req)
	if err != nil {
		mapError(c, err)
		return
	}
	obj := result.Object
	s.mutations.OnObjectCopy(ctx, src.Bucket, src.Key, dstBucket, dstKey, result)

	response := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<CopyObjectResult>
  <ETag>"%s"</ETag>
  <LastModified>%s</LastModified>
</CopyObjectResult>`, obj.ETag, time.Unix(obj.LastModified, 0).UTC().Format(time.RFC3339))
	c.Data(consts.StatusOK, "application/xml", []byte(response))
}

func parseCopySource(raw string) (storage.ObjectRef, bool) {
	raw = strings.TrimPrefix(raw, "/")
	u, err := url.Parse(raw)
	if err != nil {
		return storage.ObjectRef{}, false
	}
	path, err := url.PathUnescape(u.Path)
	if err != nil {
		return storage.ObjectRef{}, false
	}
	parts := strings.SplitN(path, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return storage.ObjectRef{}, false
	}
	return storage.ObjectRef{Bucket: parts[0], Key: parts[1], VersionID: u.Query().Get("versionId")}, true
}

func parseCopyMetadataDirective(raw string) (storage.CopyMetadataDirective, bool) {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "", string(storage.CopyMetadataCopy):
		return storage.CopyMetadataCopy, true
	case string(storage.CopyMetadataReplace):
		return storage.CopyMetadataReplace, true
	default:
		return "", false
	}
}

func copyUserMetadata(c *app.RequestContext) map[string]string {
	var metadata map[string]string
	c.Request.Header.VisitAll(func(k, v []byte) {
		key := strings.ToLower(string(k))
		if !strings.HasPrefix(key, "x-amz-meta-") {
			return
		}
		if metadata == nil {
			metadata = make(map[string]string)
		}
		metadata[key] = string(v)
	})
	return metadata
}

func writeUserMetadataHeaders(c *app.RequestContext, metadata map[string]string) {
	for k, v := range metadata {
		if strings.HasPrefix(strings.ToLower(k), "x-amz-meta-") {
			c.Header(k, v)
		}
	}
}

func copyPreconditions(c *app.RequestContext) (storage.CopyPreconditions, bool) {
	modifiedSince, ok := parseOptionalHTTPTime(string(c.GetHeader("x-amz-copy-source-if-modified-since")))
	if !ok {
		return storage.CopyPreconditions{}, false
	}
	unmodifiedSince, ok := parseOptionalHTTPTime(string(c.GetHeader("x-amz-copy-source-if-unmodified-since")))
	if !ok {
		return storage.CopyPreconditions{}, false
	}
	return storage.CopyPreconditions{
		IfMatch:           string(c.GetHeader("x-amz-copy-source-if-match")),
		IfNoneMatch:       string(c.GetHeader("x-amz-copy-source-if-none-match")),
		IfModifiedSince:   modifiedSince,
		IfUnmodifiedSince: unmodifiedSince,
	}, true
}

func parseOptionalHTTPTime(raw string) (*time.Time, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, true
	}
	t, err := http.ParseTime(raw)
	if err != nil {
		return nil, false
	}
	return &t, true
}
