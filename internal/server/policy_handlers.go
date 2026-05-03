package server

import (
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

// PolicyBackend is implemented by backends that support bucket policy storage.
type PolicyBackend interface {
	GetBucketPolicy(bucket string) ([]byte, error)
	SetBucketPolicy(bucket string, policyJSON []byte) error
	DeleteBucketPolicy(bucket string) error
}

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

	pb, ok := unwrapBackend(s.backend).(PolicyBackend)
	if !ok {
		// For backends without policy persistence, just cache in-memory
		if err := s.policyStore.Set(bucket, body); err != nil {
			writeXMLError(c, consts.StatusBadRequest, "MalformedPolicy", err.Error())
			return
		}
		c.Status(consts.StatusNoContent)
		return
	}

	if err := pb.SetBucketPolicy(bucket, body); err != nil {
		writeXMLError(c, consts.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// Update in-memory cache
	if err := s.policyStore.Set(bucket, body); err != nil {
		writeXMLError(c, consts.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	c.Status(consts.StatusNoContent)
}

func (s *Server) getBucketPolicy(c *app.RequestContext, bucket string) {
	// Try in-memory cache first
	if raw := s.policyStore.GetRaw(bucket); raw != nil {
		c.Data(consts.StatusOK, "application/json", raw)
		return
	}

	// Try backend persistence
	pb, ok := unwrapBackend(s.backend).(PolicyBackend)
	if !ok {
		writeXMLError(c, consts.StatusNotFound, "NoSuchBucketPolicy", "The bucket policy does not exist")
		return
	}

	data, err := pb.GetBucketPolicy(bucket)
	if err != nil {
		writeXMLError(c, consts.StatusNotFound, "NoSuchBucketPolicy", "The bucket policy does not exist")
		return
	}

	// Cache for future requests
	if err := s.policyStore.Set(bucket, data); err != nil {
		writeXMLError(c, consts.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	c.Data(consts.StatusOK, "application/json", data)
}

func (s *Server) deleteBucketPolicy(c *app.RequestContext, bucket string) {
	pb, ok := unwrapBackend(s.backend).(PolicyBackend)
	if !ok {
		s.policyStore.Delete(bucket)
		c.Status(consts.StatusNoContent)
		return
	}

	if err := pb.DeleteBucketPolicy(bucket); err != nil {
		writeXMLError(c, consts.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	s.policyStore.Delete(bucket)
	c.Status(consts.StatusNoContent)
}
