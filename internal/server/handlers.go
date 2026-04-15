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
	c.Header("Location", "/"+bucket)
	c.Status(consts.StatusOK)
}

// ECPolicySetter is implemented by backends that support per-bucket EC policy.
type ECPolicySetter interface {
	SetBucketECPolicy(bucket string, ecEnabled bool) error
}

func (s *Server) setBucketECPolicy(c *app.RequestContext, bucket, ecParam string) {
	setter, ok := s.backend.(ECPolicySetter)
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

func (s *Server) serveDashboard(_ context.Context, c *app.RequestContext) {
	c.SetContentType("text/html; charset=utf-8")
	c.SetStatusCode(consts.StatusOK)
	c.WriteString(dashboardHTML)
}

const dashboardHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>GrainFS Dashboard</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#0f172a;color:#e2e8f0;min-height:100vh}
.header{background:#1e293b;padding:1.5rem 2rem;border-bottom:1px solid #334155;display:flex;align-items:center;gap:1rem}
.header h1{font-size:1.5rem;font-weight:700;color:#38bdf8}
.header .badge{background:#059669;color:#fff;padding:0.25rem 0.75rem;border-radius:9999px;font-size:0.75rem;font-weight:600}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:1.5rem;padding:2rem}
.card{background:#1e293b;border-radius:0.75rem;padding:1.5rem;border:1px solid #334155}
.card h2{font-size:0.875rem;color:#94a3b8;text-transform:uppercase;letter-spacing:0.05em;margin-bottom:1rem}
.metric{font-size:2rem;font-weight:700;color:#f1f5f9}
.metric-label{font-size:0.75rem;color:#64748b;margin-top:0.25rem}
.chart-container{height:200px;display:flex;align-items:flex-end;gap:4px;padding-top:1rem}
.bar{background:linear-gradient(to top,#0ea5e9,#38bdf8);border-radius:2px 2px 0 0;min-width:8px;flex:1;transition:height 0.3s}
.status-dot{width:8px;height:8px;border-radius:50%;display:inline-block;margin-right:0.5rem}
.status-dot.healthy{background:#22c55e}
.table{width:100%;border-collapse:collapse;margin-top:0.5rem}
.table th,.table td{padding:0.5rem;text-align:left;border-bottom:1px solid #334155;font-size:0.875rem}
.table th{color:#94a3b8;font-weight:600}
#metrics-data{font-family:monospace;font-size:0.75rem;white-space:pre;overflow-x:auto;max-height:300px;overflow-y:auto;color:#94a3b8}
</style>
</head>
<body>
<div class="header">
<h1>GrainFS</h1>
<span class="badge">Dashboard</span>
</div>
<div class="grid">
<div class="card">
<h2>Server Status</h2>
<p><span class="status-dot healthy"></span>Running</p>
<div style="margin-top:1rem">
<div class="metric" id="uptime">-</div>
<div class="metric-label">Uptime</div>
</div>
</div>
<div class="card">
<h2>Requests</h2>
<div class="metric" id="total-requests">-</div>
<div class="metric-label">Total Requests</div>
<div class="chart-container" id="req-chart"></div>
</div>
<div class="card">
<h2>Storage</h2>
<div class="metric" id="storage-bytes">-</div>
<div class="metric-label">Total Bytes</div>
<table class="table" style="margin-top:1rem">
<tr><th>Buckets</th><td id="buckets-count">-</td></tr>
<tr><th>Objects</th><td id="objects-count">-</td></tr>
</table>
</div>
</div>
<script>
const startTime=Date.now();
function formatUptime(ms){const s=Math.floor(ms/1000);const m=Math.floor(s/60);const h=Math.floor(m/60);if(h>0)return h+"h "+m%60+"m";if(m>0)return m+"m "+s%60+"s";return s+"s"}
function parseMetrics(text){const m={};text.split("\n").forEach(function(l){if(l.startsWith("#")||!l.trim())return;const p=l.match(/^(\S+?)(?:\{[^}]*\})?\s+(\S+)/);if(p)m[p[1]]=(m[p[1]]||0)+parseFloat(p[2])});return m}
let reqHistory=[];
async function refresh(){try{const r=await fetch("/metrics");const t=await r.text();const m=parseMetrics(t);const total=m["grainfs_http_requests_total"]||0;document.getElementById("total-requests").textContent=Math.round(total);document.getElementById("storage-bytes").textContent=formatBytes(m["grainfs_storage_bytes_total"]||0);document.getElementById("buckets-count").textContent=Math.round(m["grainfs_buckets_total"]||0);document.getElementById("objects-count").textContent=Math.round(m["grainfs_objects_total"]||0);reqHistory.push(total);if(reqHistory.length>30)reqHistory.shift();renderChart()}catch(e){console.error(e)}document.getElementById("uptime").textContent=formatUptime(Date.now()-startTime)}
function formatBytes(b){if(b===0)return"0 B";const k=1024;const s=["B","KB","MB","GB","TB"];const i=Math.floor(Math.log(b)/Math.log(k));return(b/Math.pow(k,i)).toFixed(1)+" "+s[i]}
function renderChart(){const c=document.getElementById("req-chart");while(c.firstChild)c.removeChild(c.firstChild);const max=Math.max.apply(null,reqHistory.concat([1]));reqHistory.forEach(function(v){const bar=document.createElement("div");bar.className="bar";bar.style.height=Math.max(4,v/max*180)+"px";c.appendChild(bar)})}
setInterval(refresh,2000);refresh();
</script>
</body>
</html>`

// hertzResponseWriter adapts Hertz RequestContext to http.ResponseWriter for stdlib handlers.
type hertzResponseWriter struct {
	c          *app.RequestContext
	statusCode int
	written    bool
}

func newResponseWriter(c *app.RequestContext) *hertzResponseWriter {
	return &hertzResponseWriter{c: c, statusCode: http.StatusOK}
}

func (w *hertzResponseWriter) Header() http.Header {
	h := make(http.Header)
	w.c.Response.Header.VisitAll(func(key, value []byte) {
		h.Set(string(key), string(value))
	})
	return h
}

func (w *hertzResponseWriter) Write(data []byte) (int, error) {
	if !w.written {
		w.c.SetStatusCode(w.statusCode)
		w.written = true
	}
	w.c.Write(data)
	return len(data), nil
}

func (w *hertzResponseWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	w.c.SetStatusCode(statusCode)
	w.written = true
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
