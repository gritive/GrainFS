package server

import (
	"errors"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/storage"
)

func (s *Server) putBucketPolicy(c *app.RequestContext, bucket string) {
	body := c.Request.Body()
	if len(body) == 0 {
		writeXMLError(c, consts.StatusBadRequest, "MalformedPolicy", "empty request body")
		return
	}

	// Validate policy JSON
	if _, err := ParsePolicy(body); err != nil {
		writeXMLError(c, consts.StatusBadRequest, "MalformedPolicy", err.Error())
		return
	}

	if err := s.storeBucketPolicy(bucket, body); err != nil {
		writeXMLError(c, consts.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	c.Status(consts.StatusNoContent)
}

func (s *Server) getBucketPolicy(c *app.RequestContext, bucket string) {
	data, err := s.loadBucketPolicy(bucket)
	if err != nil {
		if errors.Is(err, storage.ErrUnsupportedOperation) {
			writeXMLError(c, consts.StatusNotFound, "NoSuchBucketPolicy", "The bucket policy does not exist")
			return
		}
		writeXMLError(c, consts.StatusNotFound, "NoSuchBucketPolicy", "The bucket policy does not exist")
		return
	}
	c.Data(consts.StatusOK, "application/json", data)
}

func (s *Server) deleteBucketPolicy(c *app.RequestContext, bucket string) {
	if err := s.deleteBucketPolicyStorage(bucket); err != nil {
		writeXMLError(c, consts.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	c.Status(consts.StatusNoContent)
}
