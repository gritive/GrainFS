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
	"github.com/gritive/GrainFS/internal/storage"
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
	if s.serveStripedRange(ctx, c, bucket, key, obj, start, length) {
		return true
	}
	c.Response.SetBodyStream(newReadAtRangeReader(ctx, reader, obj, bucket, key, start, length), int(length))
	c.Status(consts.StatusPartialContent)
	return true
}

// serveStripedRange serves a byte range from a single sequential object stream
// when the object is stripe-interleaved (obj.StripeBytes > 0), avoiding the
// O(N^2) re-open/re-discard amplification the ReadAt-based readAtRangeReader
// incurs on striped objects. Returns false (caller falls back to ReadAt) for
// contiguous objects, whose ReadAt does efficient segment-overlap reads. The
// version is pinned via loadObjectForGet so the streamed body matches the headers
// computed from the HEAD object.
func (s *Server) serveStripedRange(ctx context.Context, c *app.RequestContext, bucket, key string, obj *storage.Object, start, length int64) bool {
	if obj == nil || obj.StripeBytes == 0 {
		return false
	}
	rc, _, err := s.loadObjectForGet(ctx, bucket, key, obj.VersionID)
	if err != nil {
		mapError(c, err)
		return true
	}
	c.Response.SetBodyStream(&streamingRangeReader{rc: rc, skip: start, remaining: length}, int(length))
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
	if s.serveStripedRange(ctx, c, bucket, key, obj, start, length) {
		return true
	}
	c.Response.SetBodyStream(newReadAtRangeReader(ctx, reader, obj, bucket, key, start, length), int(length))
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
