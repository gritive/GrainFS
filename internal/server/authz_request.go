package server

import (
	"strings"

	"github.com/cloudwego/hertz/pkg/app"

	"github.com/gritive/GrainFS/internal/s3auth"
)

type s3AuthzRequest struct {
	Bucket string
	Key    string
	Method string
	Action s3auth.S3Action
}

func s3AuthzRequestFromHertz(c *app.RequestContext) s3AuthzRequest {
	bucket := c.Param("bucket")
	key := strings.TrimPrefix(c.Param("key"), "/")
	method := string(c.Method())
	return s3AuthzRequest{
		Bucket: bucket,
		Key:    key,
		Method: method,
		Action: s3ActionEnum(method, string(c.URI().Path()), key != "", c.QueryArgs().Has("policy")),
	}
}
