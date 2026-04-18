package server

import (
	"encoding/xml"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/lifecycle"
)

const maxLifecycleBodyBytes = 64 * 1024 // 64 KiB — enough for any reasonable lifecycle config

// putBucketLifecycle handles PUT /{bucket}?lifecycle.
func (s *Server) putBucketLifecycle(c *app.RequestContext, bucket string) {
	if s.lifecycleStore == nil {
		writeXMLError(c, consts.StatusNotImplemented, "NotImplemented", "lifecycle not configured")
		return
	}

	if err := s.backend.HeadBucket(bucket); err != nil {
		mapError(c, err)
		return
	}

	if len(c.Request.Body()) > maxLifecycleBodyBytes {
		writeXMLError(c, consts.StatusBadRequest, "EntityTooLarge", "lifecycle configuration exceeds 64 KiB limit")
		return
	}

	var cfg lifecycle.LifecycleConfiguration
	if err := xml.Unmarshal(c.Request.Body(), &cfg); err != nil {
		writeXMLError(c, consts.StatusBadRequest, "MalformedXML", "invalid lifecycle configuration XML")
		return
	}
	if err := lifecycle.Validate(&cfg); err != nil {
		writeXMLError(c, consts.StatusBadRequest, "InvalidArgument", err.Error())
		return
	}

	if err := s.lifecycleStore.Put(bucket, &cfg); err != nil {
		writeXMLError(c, consts.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	c.Status(consts.StatusOK)
}

// getBucketLifecycle handles GET /{bucket}?lifecycle.
func (s *Server) getBucketLifecycle(c *app.RequestContext, bucket string) {
	if s.lifecycleStore == nil {
		writeXMLError(c, consts.StatusNotImplemented, "NotImplemented", "lifecycle not configured")
		return
	}

	cfg, err := s.lifecycleStore.Get(bucket)
	if err != nil {
		writeXMLError(c, consts.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	if cfg == nil {
		writeXMLError(c, consts.StatusNotFound, "NoSuchLifecycleConfiguration",
			"the lifecycle configuration does not exist")
		return
	}

	data, err := xml.Marshal(cfg)
	if err != nil {
		writeXMLError(c, consts.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	c.Data(consts.StatusOK, "application/xml", data)
}

// deleteBucketLifecycle handles DELETE /{bucket}?lifecycle.
func (s *Server) deleteBucketLifecycle(c *app.RequestContext, bucket string) {
	if s.lifecycleStore == nil {
		writeXMLError(c, consts.StatusNotImplemented, "NotImplemented", "lifecycle not configured")
		return
	}

	if err := s.lifecycleStore.Delete(bucket); err != nil {
		writeXMLError(c, consts.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	c.Status(consts.StatusNoContent)
}
