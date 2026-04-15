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

func (s *Server) setBucketECPolicy(c *app.RequestContext, bucket, ecParam string) {
	setter, ok := unwrapBackend(s.backend).(ECPolicySetter)
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

func (s *Server) listObjects(_ context.Context, c *app.RequestContext) {
	bucket := c.Param("bucket")

	// GET /:bucket?policy — get bucket policy
	if c.QueryArgs().Has("policy") {
		s.getBucketPolicy(c, bucket)
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
	obj, err := s.backend.PutObject(bucket, key, body, contentType)
	if err != nil {
		mapError(c, err)
		return
	}

	if existing == nil {
		metrics.ObjectsTotal.Inc()
	} else {
		metrics.StorageBytesTotal.Add(float64(-existing.Size))
	}
	metrics.StorageBytesTotal.Add(float64(obj.Size))

	c.Header("ETag", fmt.Sprintf("\"%s\"", obj.ETag))
	c.Status(consts.StatusOK)
}

func (s *Server) getObject(_ context.Context, c *app.RequestContext) {
	bucket := c.Param("bucket")
	key := getKey(c)

	rc, obj, err := s.backend.GetObject(bucket, key)
	if err != nil {
		mapError(c, err)
		return
	}
	defer rc.Close()

	c.Header("Content-Type", obj.ContentType)
	c.Header("Content-Length", strconv.FormatInt(obj.Size, 10))
	c.Header("ETag", fmt.Sprintf("\"%s\"", obj.ETag))
	c.Header("Last-Modified", time.Unix(obj.LastModified, 0).UTC().Format(http.TimeFormat))

	data, _ := io.ReadAll(rc)
	c.Data(consts.StatusOK, obj.ContentType, data)
}

func (s *Server) headObject(_ context.Context, c *app.RequestContext) {
	bucket := c.Param("bucket")
	key := getKey(c)

	obj, err := s.backend.HeadObject(bucket, key)
	if err != nil {
		mapError(c, err)
		return
	}

	c.Header("Content-Type", obj.ContentType)
	c.Header("Content-Length", strconv.FormatInt(obj.Size, 10))
	c.Header("ETag", fmt.Sprintf("\"%s\"", obj.ETag))
	c.Header("Last-Modified", time.Unix(obj.LastModified, 0).UTC().Format(http.TimeFormat))
	c.Status(consts.StatusOK)
}

func (s *Server) deleteObject(_ context.Context, c *app.RequestContext) {
	bucket := c.Param("bucket")
	key := getKey(c)

	// Get size before deleting for metric tracking
	existing, _ := s.backend.HeadObject(bucket, key)

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
		"mode": "solo",
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
