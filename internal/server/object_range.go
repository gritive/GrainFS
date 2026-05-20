package server

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/eventstore"
	"github.com/gritive/GrainFS/internal/s3auth"
)

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

	if s.mustAuthorizePostLoad(ctx, c, bucket, key, s3auth.GetObject, obj.ACL) {
		return true
	}

	etag := s.writeObjectReadHeaders(c, obj, false)
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
		obj:     obj,
		bucket:  bucket,
		key:     key,
		offset:  start,
		length:  length,
	}, int(length))
	c.Status(consts.StatusPartialContent)
	return true
}

func (s *Server) getObjectPartNumberReadAt(ctx context.Context, c *app.RequestContext, bucket, key string, partN int) bool {
	reader, ok := s.backend.(objectReadAtBackend)
	if !ok {
		return false
	}

	obj, err := s.headObjectWithReadAfterWriteRetry(ctx, bucket, key)
	if err != nil {
		mapError(c, err)
		return true
	}

	if s.mustAuthorizePostLoad(ctx, c, bucket, key, s3auth.GetObject, obj.ACL) {
		return true
	}

	start, end, partETag, partsCount, ok := partRange(obj, partN)
	if !ok {
		c.Header("Content-Range", fmt.Sprintf("bytes */%d", obj.Size))
		writeXMLError(c, consts.StatusRequestedRangeNotSatisfiable,
			"InvalidPartNumber",
			"the requested partnumber is not satisfiable for this object")
		return true
	}

	if start == 0 && end+1 == obj.Size {
		return false
	}

	s.writeObjectReadHeaders(c, obj, false)
	etag := fmt.Sprintf("\"%s\"", partETag)
	c.Header("ETag", etag)
	c.Header("x-amz-mp-parts-count", strconv.Itoa(partsCount))
	if !checkConditionals(c, etag, obj.LastModified) {
		return true
	}

	if end < start {
		c.Header("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, obj.Size))
		c.Header("Content-Length", "0")
		c.Set(auditBytesOutKey, int64(0))
		c.Status(consts.StatusPartialContent)
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
		obj:     obj,
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
