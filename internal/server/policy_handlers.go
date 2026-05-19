package server

import (
	"errors"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/storage"
)

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
