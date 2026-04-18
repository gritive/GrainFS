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
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/metrics"
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

type s3Error struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

func writeXMLError(c *app.RequestContext, status int, code, message string) {
	c.SetContentType("application/xml")
	data, _ := xml.Marshal(s3Error{Code: code, Message: message})
	c.Data(status, "application/xml", data)
}

func mapError(c *app.RequestContext, err error) {
	switch {
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
	default:
		writeXMLError(c, consts.StatusInternalServerError, "InternalError", err.Error())
	}
}

func getKey(c *app.RequestContext) string {
	key := c.Param("key")
	return strings.TrimPrefix(key, "/")
}

func (s *Server) listBuckets(_ context.Context, c *app.RequestContext) {
	buckets, err := s.backend.ListBuckets()
	if err != nil {
		mapError(c, err)
		return
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

func (s *Server) createBucket(_ context.Context, c *app.RequestContext) {
	bucket := c.Param("bucket")

	// PUT /:bucket?policy — set bucket policy
	if c.QueryArgs().Has("policy") {
		s.putBucketPolicy(c, bucket)
		return
	}

	// PUT /:bucket?versioning — set versioning state
	if c.QueryArgs().Has("versioning") {
		s.putBucketVersioning(c, bucket)
		return
	}

	// Check if this is an EC policy update: PUT /:bucket?ec=true|false
	ecParam := string(c.QueryArgs().Peek("ec"))
	if ecParam != "" {
		s.setBucketECPolicy(c, bucket, ecParam)
		return
	}

	if err := s.backend.CreateBucket(bucket); err != nil {
		mapError(c, err)
		return
	}
	metrics.BucketsTotal.Inc()
	c.Header("Location", "/"+bucket)
	c.Status(consts.StatusOK)
}

// ECPolicySetter is implemented by backends that support per-bucket EC policy.
type ECPolicySetter interface {
	SetBucketECPolicy(bucket string, ecEnabled bool) error
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

// findECPolicySetter walks the backend chain and returns the first ECPolicySetter found.
func findECPolicySetter(b storage.Backend) (ECPolicySetter, bool) {
	for b != nil {
		if s, ok := b.(ECPolicySetter); ok {
			return s, true
		}
		u, ok := b.(unwrapper)
		if !ok {
			break
		}
		b = u.Unwrap()
	}
	return nil, false
}

func (s *Server) setBucketECPolicy(c *app.RequestContext, bucket, ecParam string) {
	setter, ok := findECPolicySetter(s.backend)
	if !ok {
		writeXMLError(c, consts.StatusNotImplemented, "NotImplemented", "EC policy not supported by this backend")
		return
	}

	ecEnabled := ecParam != "false"
	if err := setter.SetBucketECPolicy(bucket, ecEnabled); err != nil {
		mapError(c, err)
		return
	}
	c.Status(consts.StatusOK)
}

func (s *Server) headBucket(_ context.Context, c *app.RequestContext) {
	bucket := c.Param("bucket")
	if err := s.backend.HeadBucket(bucket); err != nil {
		mapError(c, err)
		return
	}
	c.Status(consts.StatusOK)
}

func (s *Server) deleteBucket(_ context.Context, c *app.RequestContext) {
	bucket := c.Param("bucket")

	// DELETE /:bucket?policy — delete bucket policy
	if c.QueryArgs().Has("policy") {
		s.deleteBucketPolicy(c, bucket)
		return
	}

	if err := s.backend.DeleteBucket(bucket); err != nil {
		mapError(c, err)
		return
	}
	metrics.BucketsTotal.Dec()
	c.Status(consts.StatusNoContent)
}

func (s *Server) listObjects(ctx context.Context, c *app.RequestContext) {
	bucket := c.Param("bucket")

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

	prefix := string(c.QueryArgs().Peek("prefix"))
	maxKeys := 1000
	if mk := string(c.QueryArgs().Peek("max-keys")); mk != "" {
		if v, err := strconv.Atoi(mk); err == nil && v > 0 {
			maxKeys = v
		}
	}

	objects, err := s.backend.ListObjects(bucket, prefix, maxKeys)
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

func (s *Server) handlePut(_ context.Context, c *app.RequestContext) {
	bucket := c.Param("bucket")
	key := getKey(c)

	// Check if this is an UploadPart request
	uploadID := string(c.QueryArgs().Peek("uploadId"))
	partNumberStr := string(c.QueryArgs().Peek("partNumber"))
	if uploadID != "" && partNumberStr != "" {
		s.uploadPart(c, bucket, key, uploadID, partNumberStr)
		return
	}

	// Check for CopyObject (PUT with x-amz-copy-source header)
	copySource := string(c.GetHeader("x-amz-copy-source"))
	if copySource != "" {
		s.handleCopyObject(c, bucket, key, copySource)
		return
	}

	contentType := string(c.GetHeader("Content-Type"))
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	// Check if object already exists (for overwrite — don't double-count)
	existing, _ := s.backend.HeadObject(bucket, key)

	rawBody := c.Request.Body()

	// Handle aws-chunked Content-Encoding (used by AWS SDKs for streaming uploads)
	contentEncoding := string(c.GetHeader("Content-Encoding"))
	contentSHA := string(c.GetHeader("X-Amz-Content-Sha256"))
	if contentEncoding == "aws-chunked" || contentSHA == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" {
		decoded, err := s3auth.DecodeAWSChunkedBody(rawBody)
		if err != nil {
			c.AbortWithMsg(fmt.Sprintf("invalid aws-chunked encoding: %v", err), 400)
			return
		}
		rawBody = decoded
	}

	body := bytes.NewReader(rawBody)
	aclHeader := string(c.GetHeader("x-amz-acl"))

	var (
		obj    *storage.Object
		putErr error
	)
	if aclHeader != "" {
		acl := s3auth.ParseACLHeader(aclHeader)
		if putter, ok := unwrapBackend(s.backend).(storage.AtomicACLPutter); ok {
			// Atomic path: store object and ACL in the same transaction.
			obj, putErr = putter.PutObjectWithACL(bucket, key, body, contentType, uint8(acl))
		} else {
			obj, putErr = s.backend.PutObject(bucket, key, body, contentType)
			if putErr == nil {
				if setter, ok2 := unwrapBackend(s.backend).(storage.ACLSetter); ok2 {
					if aclErr := setter.SetObjectACL(bucket, key, uint8(acl)); aclErr != nil {
						mapError(c, aclErr)
						return
					}
				}
			}
		}
	} else {
		obj, putErr = s.backend.PutObject(bucket, key, body, contentType)
	}
	if putErr != nil {
		mapError(c, putErr)
		return
	}

	if existing == nil {
		metrics.ObjectsTotal.Inc()
	} else {
		metrics.StorageBytesTotal.Add(float64(-existing.Size))
	}
	metrics.StorageBytesTotal.Add(float64(obj.Size))

	c.Header("ETag", fmt.Sprintf("\"%s\"", obj.ETag))
	if obj.VersionID != "" {
		c.Header("X-Amz-Version-Id", obj.VersionID)
	}
	c.Status(consts.StatusOK)
}

// VersionedGetter is an optional interface for backends supporting retrieval by versionId.
type VersionedGetter interface {
	GetObjectVersion(bucket, key, versionID string) (io.ReadCloser, *storage.Object, error)
}

func (s *Server) getObject(ctx context.Context, c *app.RequestContext) {
	bucket := c.Param("bucket")
	key := getKey(c)

	versionID := string(c.QueryArgs().Peek("versionId"))
	var rc io.ReadCloser
	var obj *storage.Object
	var err error

	if versionID != "" {
		if vg, ok := s.backend.(VersionedGetter); ok {
			rc, obj, err = vg.GetObjectVersion(bucket, key, versionID)
		} else {
			// Walk unwrappers
			b := s.backend
			for b != nil {
				if vg, ok := b.(VersionedGetter); ok {
					rc, obj, err = vg.GetObjectVersion(bucket, key, versionID)
					break
				}
				u, ok := b.(unwrapper)
				if !ok {
					writeXMLError(c, consts.StatusNotImplemented, "NotImplemented", "versionId not supported by this backend")
					return
				}
				b = u.Unwrap()
			}
		}
	} else {
		rc, obj, err = s.backend.GetObject(bucket, key)
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

	// ACL secondary check: anonymous requests require public-read or public-read-write ACL.
	if s.verifier != nil {
		accessKey := AccessKeyFromContext(ctx)
		if !s3auth.IsAuthorizedByACL(s3auth.ACLGrant(obj.ACL), accessKey, s3auth.GetObject) {
			writeXMLError(c, consts.StatusForbidden, "AccessDenied", "Access Denied")
			return
		}
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

	if !checkConditionals(c, etag, obj.LastModified) {
		return
	}

	// Handle Range requests: must use standard path (sendfile transfers entire file)
	rangeHeader := string(c.GetHeader("Range"))
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
		data := make([]byte, length)
		if _, err := io.ReadFull(rc, data); err != nil {
			mapError(c, err)
			return
		}

		c.Header("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, obj.Size))
		c.Header("Content-Length", strconv.FormatInt(length, 10))
		c.Data(consts.StatusPartialContent, obj.ContentType, data)
		return
	}

	// Zero-copy for large non-range requests
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

// VersionedHeader is an optional interface for backends supporting HEAD by versionId.
type VersionedHeader interface {
	HeadObjectVersion(bucket, key, versionID string) (*storage.Object, error)
}

func findVersionedHeader(b storage.Backend) (VersionedHeader, bool) {
	for b != nil {
		if v, ok := b.(VersionedHeader); ok {
			return v, true
		}
		u, ok := b.(unwrapper)
		if !ok {
			break
		}
		b = u.Unwrap()
	}
	return nil, false
}

func (s *Server) headObject(ctx context.Context, c *app.RequestContext) {
	bucket := c.Param("bucket")
	key := getKey(c)

	versionID := string(c.QueryArgs().Peek("versionId"))
	var obj *storage.Object
	var err error
	if versionID != "" {
		vh, ok := findVersionedHeader(s.backend)
		if !ok {
			writeXMLError(c, consts.StatusNotImplemented, "NotImplemented", "versionId not supported by this backend")
			return
		}
		obj, err = vh.HeadObjectVersion(bucket, key, versionID)
	} else {
		obj, err = s.backend.HeadObject(bucket, key)
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

	// ACL secondary check: anonymous requests require public-read or public-read-write ACL.
	if s.verifier != nil {
		accessKey := AccessKeyFromContext(ctx)
		if !s3auth.IsAuthorizedByACL(s3auth.ACLGrant(obj.ACL), accessKey, s3auth.HeadObject) {
			writeXMLError(c, consts.StatusForbidden, "AccessDenied", "Access Denied")
			return
		}
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

func (s *Server) deleteObject(_ context.Context, c *app.RequestContext) {
	bucket := c.Param("bucket")
	key := getKey(c)

	// DELETE /:bucket/:key?versionId=<id> — hard-delete specific version
	if versionID := string(c.QueryArgs().Peek("versionId")); versionID != "" {
		vd, ok := findVersionDeleter(s.backend)
		if !ok {
			writeXMLError(c, consts.StatusNotImplemented, "NotImplemented", "versionId delete not supported by this backend")
			return
		}
		if err := vd.DeleteObjectVersion(bucket, key, versionID); err != nil {
			mapError(c, err)
			return
		}
		c.Status(consts.StatusNoContent)
		return
	}

	// Get size before deleting for metric tracking
	existing, _ := s.backend.HeadObject(bucket, key)

	// If backend supports versioned soft-delete, use it to get the marker ID.
	if vsd, ok := findVersionedSoftDeleter(s.backend); ok {
		markerID, err := vsd.DeleteObjectReturningMarker(bucket, key)
		if err != nil {
			mapError(c, err)
			return
		}
		if markerID != "" {
			c.Header("x-amz-delete-marker", "true")
			c.Header("x-amz-version-id", markerID)
		}
		metrics.ObjectsTotal.Dec()
		if existing != nil {
			metrics.StorageBytesTotal.Add(float64(-existing.Size))
		}
		c.Status(consts.StatusNoContent)
		return
	}

	if err := s.backend.DeleteObject(bucket, key); err != nil {
		mapError(c, err)
		return
	}
	metrics.ObjectsTotal.Dec()
	if existing != nil {
		metrics.StorageBytesTotal.Add(float64(-existing.Size))
	}
	c.Status(consts.StatusNoContent)
}

// handlePost routes POST requests for multipart upload operations.
func (s *Server) handlePost(_ context.Context, c *app.RequestContext) {
	bucket := c.Param("bucket")
	key := getKey(c)

	// POST /:bucket/:key?uploads -> CreateMultipartUpload
	if c.QueryArgs().Has("uploads") {
		s.createMultipartUpload(c, bucket, key)
		return
	}

	// POST /:bucket/:key?uploadId=xxx -> CompleteMultipartUpload
	uploadID := string(c.QueryArgs().Peek("uploadId"))
	if uploadID != "" {
		s.completeMultipartUpload(c, bucket, key, uploadID)
		return
	}

	// POST /:bucket with multipart/form-data -> Form-based Upload (POST Policy)
	ct := string(c.GetHeader("Content-Type"))
	if strings.HasPrefix(ct, "multipart/form-data") {
		s.handleFormUpload(c, bucket)
		return
	}

	writeXMLError(c, consts.StatusBadRequest, "InvalidRequest", "unsupported POST operation")
}

// handleFormUpload processes S3 POST form-based uploads (browser direct upload).
// The form contains: key, Content-Type, policy, X-Amz-Signature, file, etc.
func (s *Server) handleFormUpload(c *app.RequestContext, bucket string) {
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

	// Validate POST policy if authentication is enabled and policy is present
	if s.verifier != nil {
		policyB64 := ""
		if ps := form.Value["policy"]; len(ps) > 0 {
			policyB64 = ps[0]
		}
		sig := ""
		if ss := form.Value["X-Amz-Signature"]; len(ss) > 0 {
			sig = ss[0]
		}

		if policyB64 != "" {
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

			// Validate signature
			if sig != "" {
				credential := ""
				if cs := form.Value["X-Amz-Credential"]; len(cs) > 0 {
					credential = cs[0]
				}
				// credential format: AKID/20260416/us-east-1/s3/aws4_request
				parts := strings.SplitN(credential, "/", 5)
				if len(parts) == 5 {
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
			}
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

	obj, err := s.backend.PutObject(bucket, key, file, contentType)
	if err != nil {
		mapError(c, err)
		return
	}

	metrics.ObjectsTotal.Inc()
	metrics.StorageBytesTotal.Add(float64(obj.Size))

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

func (s *Server) createMultipartUpload(c *app.RequestContext, bucket, key string) {
	contentType := string(c.GetHeader("Content-Type"))
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	upload, err := s.backend.CreateMultipartUpload(bucket, key, contentType)
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

func (s *Server) uploadPart(c *app.RequestContext, bucket, key, uploadID, partNumberStr string) {
	partNumber, err := strconv.Atoi(partNumberStr)
	if err != nil || partNumber < 1 {
		writeXMLError(c, consts.StatusBadRequest, "InvalidArgument", "invalid part number")
		return
	}

	body := bytes.NewReader(c.Request.Body())
	part, err := s.backend.UploadPart(bucket, key, uploadID, partNumber, body)
	if err != nil {
		mapError(c, err)
		return
	}

	c.Header("ETag", fmt.Sprintf("\"%s\"", part.ETag))
	c.Status(consts.StatusOK)
}

func (s *Server) completeMultipartUpload(c *app.RequestContext, bucket, key, uploadID string) {
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

	obj, err := s.backend.CompleteMultipartUpload(bucket, key, uploadID, parts)
	if err != nil {
		mapError(c, err)
		return
	}
	metrics.ObjectsTotal.Inc()
	metrics.StorageBytesTotal.Add(float64(obj.Size))

	result := completeMultipartUploadResult{
		Xmlns:  "http://s3.amazonaws.com/doc/2006-03-01/",
		Bucket: bucket,
		Key:    key,
		ETag:   fmt.Sprintf("\"%s\"", obj.ETag),
	}
	data, _ := xml.Marshal(result)
	c.Data(consts.StatusOK, "application/xml", data)
}

func (s *Server) serveDashboard(_ context.Context, c *app.RequestContext) {
	data, err := uiHTML.ReadFile("ui/index.html")
	if err != nil {
		c.String(consts.StatusInternalServerError, "UI not found")
		return
	}
	c.SetContentType("text/html; charset=utf-8")
	c.SetStatusCode(consts.StatusOK)
	c.Write(data)
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
	w.c.Write(data)
	return len(data), nil
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

func (s *Server) clusterStatus(_ context.Context, c *app.RequestContext) {
	status := map[string]any{
		"mode":                 "solo",
		"split_brain_suspected": false,
	}

	if s.cluster != nil {
		status["mode"] = "cluster"
		status["node_id"] = s.cluster.NodeID()
		status["state"] = s.cluster.State()
		status["term"] = s.cluster.Term()
		status["leader_id"] = s.cluster.LeaderID()
		status["peers"] = s.cluster.Peers()
	}

	data, _ := json.Marshal(status)
	c.Data(consts.StatusOK, "application/json", data)
}

// joinClusterHandler handles POST /api/cluster/join for runtime solo→cluster transition.
func (s *Server) joinClusterHandler(_ context.Context, c *app.RequestContext) {
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

	resp, _ := json.Marshal(map[string]string{"status": "joined", "mode": "cluster"})
	c.Data(consts.StatusOK, "application/json", resp)
}

// handleCopyObject processes PUT with x-amz-copy-source header (S3 CopyObject).
func (s *Server) handleCopyObject(c *app.RequestContext, dstBucket, dstKey, copySource string) {
	// Parse copy source: /bucket/key or bucket/key
	copySource = strings.TrimPrefix(copySource, "/")
	parts := strings.SplitN(copySource, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		writeXMLError(c, consts.StatusBadRequest, "InvalidArgument", "invalid x-amz-copy-source format")
		return
	}
	srcBucket, srcKey := parts[0], parts[1]

	// Try optimized copy if backend supports it
	if copier, ok := s.backend.(storage.Copier); ok {
		obj, err := copier.CopyObject(srcBucket, srcKey, dstBucket, dstKey)
		if err != nil {
			mapError(c, err)
			return
		}

		result := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<CopyObjectResult>
  <ETag>"%s"</ETag>
  <LastModified>%s</LastModified>
</CopyObjectResult>`, obj.ETag, time.Unix(obj.LastModified, 0).UTC().Format(time.RFC3339))
		c.Data(consts.StatusOK, "application/xml", []byte(result))
		return
	}

	// Fallback: read source, write to dest
	rc, obj, err := s.backend.GetObject(srcBucket, srcKey)
	if err != nil {
		mapError(c, err)
		return
	}
	defer rc.Close()

	newObj, err := s.backend.PutObject(dstBucket, dstKey, rc, obj.ContentType)
	if err != nil {
		mapError(c, err)
		return
	}

	result := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<CopyObjectResult>
  <ETag>"%s"</ETag>
  <LastModified>%s</LastModified>
</CopyObjectResult>`, newObj.ETag, time.Unix(newObj.LastModified, 0).UTC().Format(time.RFC3339))
	c.Data(consts.StatusOK, "application/xml", []byte(result))
}
