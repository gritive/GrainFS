package server

import (
	"bytes"
	"context"
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
	if err := s.backend.CreateBucket(bucket); err != nil {
		mapError(c, err)
		return
	}
	c.Header("Location", "/"+bucket)
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
	if err := s.backend.DeleteBucket(bucket); err != nil {
		mapError(c, err)
		return
	}
	c.Status(consts.StatusNoContent)
}

func (s *Server) listObjects(_ context.Context, c *app.RequestContext) {
	bucket := c.Param("bucket")
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

	body := bytes.NewReader(c.Request.Body())
	obj, err := s.backend.PutObject(bucket, key, body, contentType)
	if err != nil {
		mapError(c, err)
		return
	}

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

	if err := s.backend.DeleteObject(bucket, key); err != nil {
		mapError(c, err)
		return
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

	writeXMLError(c, consts.StatusBadRequest, "InvalidRequest", "unsupported POST operation")
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

	result := completeMultipartUploadResult{
		Xmlns:  "http://s3.amazonaws.com/doc/2006-03-01/",
		Bucket: bucket,
		Key:    key,
		ETag:   fmt.Sprintf("\"%s\"", obj.ETag),
	}
	data, _ := xml.Marshal(result)
	c.Data(consts.StatusOK, "application/xml", data)
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
