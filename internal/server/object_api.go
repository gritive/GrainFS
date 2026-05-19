package server

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/eventstore"
	"github.com/gritive/GrainFS/internal/s3auth"
	"github.com/gritive/GrainFS/internal/storage"
)

func getKey(c *app.RequestContext) string {
	key := c.Param("key")
	return strings.TrimPrefix(key, "/")
}

func (s *Server) getObject(ctx context.Context, c *app.RequestContext) {
	if err := s.waitForReadIndex(ctx); err != nil {
		mapError(c, err)
		return
	}

	bucket := c.Param("bucket")
	key := getKey(c)
	if key == "" {
		s.listObjects(ctx, c)
		return
	}

	if c.QueryArgs().Has("tagging") {
		s.getObjectTagging(ctx, c)
		return
	}

	// GET /:bucket/:key?uploadId=<id> — list parts for one in-progress multipart.
	// Checked before versionId / Range because S3 routes the request to ListParts
	// whenever uploadId is present, even when other query strings appear.
	if uploadID := string(c.QueryArgs().Peek("uploadId")); uploadID != "" {
		s.listParts(ctx, c, bucket, key, uploadID)
		return
	}

	versionID := string(c.QueryArgs().Peek("versionId"))
	rangeHeader := string(c.GetHeader("Range"))
	partN, handled := readPartNumber(c, rangeHeader)
	if handled && partN < 0 {
		return
	}
	if partN == 0 && versionID == "" && rangeHeader != "" {
		if s.getObjectRangeReadAt(ctx, c, bucket, key, rangeHeader) {
			return
		}
	}

	rc, obj, err := s.loadObjectForGet(ctx, bucket, key, versionID)
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
	etag := s.writeObjectReadHeaders(c, obj, false)

	if partN > 0 {
		start, end, partETag, partsCount, ok := partRange(obj, partN)
		if !ok {
			c.Header("Content-Range", fmt.Sprintf("bytes */%d", obj.Size))
			writeXMLError(c, consts.StatusRequestedRangeNotSatisfiable,
				"InvalidPartNumber",
				"the requested partnumber is not satisfiable for this object")
			return
		}
		etag = fmt.Sprintf("\"%s\"", partETag)
		c.Header("ETag", etag)
		c.Header("x-amz-mp-parts-count", strconv.Itoa(partsCount))
		// Zero-byte parts (Size==0) have end<start by construction; the
		// Range body path rejects that and 416s. Short-circuit here with
		// an explicit empty 206 + Content-Range/Content-Length so the
		// GET response is a real "no bytes for this part".
		if end < start {
			if !checkConditionals(c, etag, obj.LastModified) {
				return
			}
			c.Header("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, obj.Size))
			c.Header("Content-Length", "0")
			c.Status(consts.StatusPartialContent)
			return
		}
		rangeHeader = fmt.Sprintf("bytes=%d-%d", start, end)
	}

	if !checkConditionals(c, etag, obj.LastModified) {
		return
	}

	bodyStreamOwnsReader, err := writeObjectBody(c, rc, obj, rangeHeader)
	if err != nil {
		mapError(c, err)
		return
	}
	if bodyStreamOwnsReader {
		rc = nil
	}
}
