package server

import (
	"context"
	"errors"
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
