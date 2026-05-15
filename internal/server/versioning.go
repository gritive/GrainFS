package server

import (
	"context"
	"errors"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

var errInvalidBucketVersioningStatus = errors.New("invalid bucket versioning status")

// putBucketVersioning handles PUT /<bucket>?versioning.
func (s *Server) putBucketVersioning(c *app.RequestContext, bucket string) {
	status, err := parseBucketVersioningConfiguration(c.Request.Body())
	if err != nil {
		if errors.Is(err, errInvalidBucketVersioningStatus) {
			writeXMLError(c, consts.StatusBadRequest, "InvalidArgument", "versioning status must be Enabled or Suspended")
			return
		}
		writeXMLError(c, consts.StatusBadRequest, "MalformedXML", "invalid versioning configuration XML")
		return
	}

	if err := s.setBucketVersioning(bucket, status); err != nil {
		mapError(c, err)
		return
	}
	c.Status(consts.StatusOK)
}

// listObjectVersions handles GET /<bucket>?versions.
func (s *Server) listObjectVersions(_ context.Context, c *app.RequestContext, bucket string) {
	prefix := string(c.QueryArgs().Peek("prefix"))
	maxKeys := 1000

	vs, err := s.loadObjectVersions(bucket, prefix, maxKeys)
	if err != nil {
		mapError(c, err)
		return
	}

	writeListVersionsResult(c, buildListVersionsResult(bucket, prefix, maxKeys, vs))
}

// getBucketVersioning handles GET /<bucket>?versioning.
func (s *Server) getBucketVersioning(_ context.Context, c *app.RequestContext, bucket string) {
	state, err := s.getBucketVersioningState(bucket)
	if err != nil {
		mapError(c, err)
		return
	}

	writeBucketVersioningConfiguration(c, state)
}
