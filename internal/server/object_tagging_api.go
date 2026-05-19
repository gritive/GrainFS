package server

import (
	"context"
	"errors"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/storage/tagging"
)

const maxTaggingBodyBytes = 64 * 1024

func (s *Server) putObjectTagging(ctx context.Context, c *app.RequestContext) {
	bucket := c.Param("bucket")
	key := getKey(c)
	versionID := string(c.QueryArgs().Peek("versionId"))

	body := c.Request.Body()
	if len(body) > maxTaggingBodyBytes {
		writeXMLError(c, consts.StatusBadRequest, "EntityTooLarge", "tagging body too large")
		return
	}
	tags, err := ParseTaggingXML(body)
	if err != nil {
		metrics.ObjectTaggingValidationErrors.WithLabelValues("xml").Inc()
		metrics.ObjectTaggingRequests.WithLabelValues("put", "err").Inc()
		writeXMLError(c, consts.StatusBadRequest, "MalformedXML", err.Error())
		return
	}
	if err := tagging.Validate(tags); err != nil {
		metrics.ObjectTaggingValidationErrors.WithLabelValues(reasonLabel(err)).Inc()
		metrics.ObjectTaggingRequests.WithLabelValues("put", "err").Inc()
		writeXMLError(c, consts.StatusBadRequest, "InvalidTag", err.Error())
		return
	}
	if err := s.ops.SetObjectTags(bucket, key, versionID, tags); err != nil {
		metrics.ObjectTaggingRequests.WithLabelValues("put", "err").Inc()
		mapError(c, err)
		return
	}
	metrics.ObjectTaggingRequests.WithLabelValues("put", "ok").Inc()
	metrics.ObjectTagsPerObject.Observe(float64(len(tags)))
	c.Status(consts.StatusOK)
}

func (s *Server) getObjectTagging(_ context.Context, c *app.RequestContext) {
	bucket := c.Param("bucket")
	key := getKey(c)
	versionID := string(c.QueryArgs().Peek("versionId"))

	tags, err := s.ops.GetObjectTags(bucket, key, versionID)
	if err != nil {
		metrics.ObjectTaggingRequests.WithLabelValues("get", "err").Inc()
		mapError(c, err)
		return
	}
	out, err := MarshalTaggingXML(tags)
	if err != nil {
		metrics.ObjectTaggingRequests.WithLabelValues("get", "err").Inc()
		writeXMLError(c, consts.StatusInternalServerError, "InternalError", "marshal tagging")
		return
	}
	metrics.ObjectTaggingRequests.WithLabelValues("get", "ok").Inc()
	c.Data(consts.StatusOK, "application/xml", out)
}

func (s *Server) deleteObjectTagging(_ context.Context, c *app.RequestContext) {
	bucket := c.Param("bucket")
	key := getKey(c)
	versionID := string(c.QueryArgs().Peek("versionId"))

	if err := s.ops.DeleteObjectTags(bucket, key, versionID); err != nil {
		metrics.ObjectTaggingRequests.WithLabelValues("delete", "err").Inc()
		mapError(c, err)
		return
	}
	metrics.ObjectTaggingRequests.WithLabelValues("delete", "ok").Inc()
	c.Status(consts.StatusNoContent)
}

func reasonLabel(err error) string {
	switch {
	case errors.Is(err, tagging.ErrTagCount):
		return "count"
	case errors.Is(err, tagging.ErrTagKeyLen):
		return "key_len"
	case errors.Is(err, tagging.ErrTagValueLen):
		return "value_len"
	case errors.Is(err, tagging.ErrTagCharset):
		return "charset"
	case errors.Is(err, tagging.ErrReservedTag):
		return "reserved"
	case errors.Is(err, tagging.ErrDuplicateTag):
		return "duplicate"
	default:
		return "other"
	}
}
