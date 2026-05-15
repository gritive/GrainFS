package server

import (
	"context"
	"errors"

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

	if !checkConditionals(c, etag, obj.LastModified) {
		return
	}

	c.Status(consts.StatusOK)
}
