package server

import (
	"context"
	"encoding/xml"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

type objectLockConfigurationResult struct {
	XMLName           xml.Name `xml:"ObjectLockConfiguration"`
	Xmlns             string   `xml:"xmlns,attr,omitempty"`
	ObjectLockEnabled string   `xml:"ObjectLockEnabled"`
}

type objectRetentionRequest struct {
	XMLName         xml.Name `xml:"Retention"`
	Mode            string   `xml:"Mode"`
	RetainUntilDate string   `xml:"RetainUntilDate"`
}

func (s *Server) getBucketObjectLockConfiguration(ctx context.Context, c *app.RequestContext, bucket string) {
	if err := s.requireBucket(ctx, bucket); err != nil {
		mapError(c, err)
		return
	}

	data, _ := xml.Marshal(objectLockConfigurationResult{
		Xmlns:             "http://s3.amazonaws.com/doc/2006-03-01/",
		ObjectLockEnabled: "Enabled",
	})
	c.Data(consts.StatusOK, "application/xml", data)
}

func (s *Server) putObjectRetention(ctx context.Context, c *app.RequestContext) {
	bucket := c.Param("bucket")
	key := getKey(c)
	versionID := string(c.QueryArgs().Peek("versionId"))

	if _, err := s.loadObjectForHead(ctx, bucket, key, versionID); err != nil {
		mapError(c, err)
		return
	}

	var req objectRetentionRequest
	if err := xml.Unmarshal(c.Request.Body(), &req); err != nil {
		writeXMLError(c, consts.StatusBadRequest, "MalformedXML", "The XML you provided was not well-formed or did not validate against our published schema.")
		return
	}
	switch req.Mode {
	case "GOVERNANCE", "COMPLIANCE":
	default:
		writeXMLError(c, consts.StatusBadRequest, "InvalidArgument", "retention mode must be GOVERNANCE or COMPLIANCE")
		return
	}
	if req.RetainUntilDate == "" {
		writeXMLError(c, consts.StatusBadRequest, "InvalidArgument", "RetainUntilDate is required")
		return
	}

	c.Status(consts.StatusOK)
}
