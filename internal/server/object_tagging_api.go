package server

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/storage/tagging"
)

func (s *Server) putObjectTagging(ctx context.Context, c *app.RequestContext) {
	bucket := c.Param("bucket")
	key := getKey(c)
	versionID := string(c.QueryArgs().Peek("versionId"))

	body := c.Request.Body()
	tags, err := ParseTaggingXML(body)
	if err != nil {
		writeXMLError(c, consts.StatusBadRequest, "MalformedXML", err.Error())
		return
	}
	if err := tagging.Validate(tags); err != nil {
		writeXMLError(c, consts.StatusBadRequest, "InvalidTag", err.Error())
		return
	}
	if err := s.ops.SetObjectTags(bucket, key, versionID, tags); err != nil {
		mapError(c, err)
		return
	}
	c.Status(consts.StatusOK)
}

func (s *Server) getObjectTagging(_ context.Context, c *app.RequestContext) {
	bucket := c.Param("bucket")
	key := getKey(c)
	versionID := string(c.QueryArgs().Peek("versionId"))

	tags, err := s.ops.GetObjectTags(bucket, key, versionID)
	if err != nil {
		mapError(c, err)
		return
	}
	out, err := MarshalTaggingXML(tags)
	if err != nil {
		writeXMLError(c, consts.StatusInternalServerError, "InternalError", "marshal tagging")
		return
	}
	c.Data(consts.StatusOK, "application/xml", out)
}

func (s *Server) deleteObjectTagging(_ context.Context, c *app.RequestContext) {
	bucket := c.Param("bucket")
	key := getKey(c)
	versionID := string(c.QueryArgs().Peek("versionId"))

	if err := s.ops.DeleteObjectTags(bucket, key, versionID); err != nil {
		mapError(c, err)
		return
	}
	c.Status(consts.StatusNoContent)
}
