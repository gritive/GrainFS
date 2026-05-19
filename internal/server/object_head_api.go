package server

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/s3auth"
	"github.com/gritive/GrainFS/internal/storage"
)

func (s *Server) headObject(ctx context.Context, c *app.RequestContext) {
	if err := s.waitForReadIndex(ctx); err != nil {
		mapError(c, err)
		return
	}

	bucket := c.Param("bucket")
	key := getKey(c)
	if key == "" {
		s.headBucket(ctx, c)
		return
	}

	versionID := string(c.QueryArgs().Peek("versionId"))
	rangeHeader := string(c.GetHeader("Range"))
	partN, handled := readPartNumber(c, rangeHeader)
	if handled && partN < 0 {
		return
	}
	obj, err := s.loadObjectForHead(ctx, bucket, key, versionID)
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

	etag := s.writeObjectReadHeaders(c, obj, true)

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
		c.Header("Content-Length", strconv.FormatInt(end-start+1, 10))
		c.Header("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, obj.Size))
	}

	if !checkConditionals(c, etag, obj.LastModified) {
		return
	}

	c.Status(consts.StatusOK)
}
