package server

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

const maxLifecycleBodyBytes = 64 * 1024

func (s *Server) putBucketLifecycle(ctx context.Context, c *app.RequestContext, bucket string) {
	if s.blockIfMutationDisabled(c, "bucket_lifecycle_put") {
		return
	}
	if !s.routeFeatureAvailable(routeFeatureLifecycle) {
		writeXMLError(c, consts.StatusNotImplemented, "NotImplemented", "lifecycle not configured")
		return
	}
	body := c.Request.Body()
	if len(body) > maxLifecycleBodyBytes {
		writeXMLError(c, consts.StatusBadRequest, "EntityTooLarge", "lifecycle configuration exceeds 64 KiB limit")
		return
	}
	if err := s.applyBucketLifecycle(ctx, bucket, body); err != nil {
		if isLifecycleBucketLookupError(err) {
			mapError(c, err)
			return
		}
		writeXMLError(c, consts.StatusBadRequest, "InvalidArgument", err.Error())
		return
	}
	c.Status(consts.StatusOK)
}

func (s *Server) getBucketLifecycle(ctx context.Context, c *app.RequestContext, bucket string) {
	_ = ctx
	if !s.routeFeatureAvailable(routeFeatureLifecycle) {
		writeXMLError(c, consts.StatusNotImplemented, "NotImplemented", "lifecycle not configured")
		return
	}
	raw, err := s.loadBucketLifecycleRaw(bucket)
	if err != nil {
		writeXMLError(c, consts.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	if raw == nil {
		writeXMLError(c, consts.StatusNotFound, "NoSuchLifecycleConfiguration",
			"the lifecycle configuration does not exist")
		return
	}
	c.Data(consts.StatusOK, "application/xml", raw)
}

func (s *Server) deleteBucketLifecycle(ctx context.Context, c *app.RequestContext, bucket string) {
	if s.blockIfMutationDisabled(c, "bucket_lifecycle_delete") {
		return
	}
	if !s.routeFeatureAvailable(routeFeatureLifecycle) {
		writeXMLError(c, consts.StatusNotImplemented, "NotImplemented", "lifecycle not configured")
		return
	}
	if err := s.removeBucketLifecycle(ctx, bucket); err != nil {
		writeXMLError(c, consts.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	c.Status(consts.StatusNoContent)
}
