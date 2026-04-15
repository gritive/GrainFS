package server

import (
	"context"
	"strings"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

// s3Action maps an HTTP method + path context to an S3 action string.
func s3Action(method, path string, hasKey bool) string {
	switch method {
	case "GET":
		if hasKey {
			return "s3:GetObject"
		}
		return "s3:ListBucket"
	case "HEAD":
		if hasKey {
			return "s3:GetObject"
		}
		return "s3:ListBucket"
	case "PUT":
		if hasKey {
			return "s3:PutObject"
		}
		return "s3:CreateBucket"
	case "DELETE":
		if hasKey {
			return "s3:DeleteObject"
		}
		return "s3:DeleteBucket"
	case "POST":
		return "s3:PutObject" // multipart
	default:
		return "s3:Unknown"
	}
}

// authzMiddleware checks bucket policies for authorized access.
// Must run after authMiddleware (which sets the access key in context).
func (s *Server) authzMiddleware() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		path := string(c.URI().Path())

		// Skip non-S3 paths
		if path == "/" || path == "/metrics" || strings.HasPrefix(path, "/ui/") || strings.HasPrefix(path, "/volumes") {
			c.Next(ctx)
			return
		}

		// Extract bucket and key from path
		bucket := c.Param("bucket")
		key := strings.TrimPrefix(c.Param("key"), "/")

		if bucket == "" {
			c.Next(ctx)
			return
		}

		// Check for policy CRUD: GET/PUT/DELETE /:bucket?policy
		if c.Query("policy") != "" || string(c.QueryArgs().Peek("policy")) == "" && c.QueryArgs().Has("policy") {
			c.Next(ctx)
			return
		}

		accessKey := AccessKeyFromContext(ctx)
		method := string(c.Method())
		action := s3Action(method, path, key != "")

		if !s.policyStore.IsAllowed(accessKey, action, bucket, key) {
			writeXMLError(c, consts.StatusForbidden, "AccessDenied", "bucket policy denies this action")
			c.Abort()
			return
		}

		c.Next(ctx)
	}
}
