package server

import (
	"context"
	"strings"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/s3auth"
)

// s3ActionEnum maps an HTTP method + path context to an S3Action enum value.
// path is required to distinguish sub-resource operations (e.g., ?delete).
func s3ActionEnum(method, path string, hasKey bool) s3auth.S3Action {
	switch method {
	case "GET":
		if hasKey {
			return s3auth.GetObject
		}
		return s3auth.ListBucket
	case "HEAD":
		if hasKey {
			return s3auth.HeadObject
		}
		return s3auth.ListBucket
	case "PUT":
		if hasKey {
			return s3auth.PutObject
		}
		return s3auth.CreateBucket
	case "DELETE":
		if hasKey {
			return s3auth.DeleteObject
		}
		return s3auth.DeleteBucket
	case "POST":
		return s3auth.PutObject // multipart upload
	default:
		return s3auth.UnknownAction
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

		// Skip policy CRUD endpoints — handled by dedicated handlers
		if c.Query("policy") != "" || string(c.QueryArgs().Peek("policy")) == "" && c.QueryArgs().Has("policy") {
			c.Next(ctx)
			return
		}

		accessKey := AccessKeyFromContext(ctx)
		in := s3auth.PermCheckInput{
			Principal: s3auth.Principal{AccessKey: accessKey},
			Resource:  s3auth.ResourceRef{Bucket: bucket, Key: key},
			Action:    s3ActionEnum(string(c.Method()), path, key != ""),
		}

		if !s.policyStore.Allow(ctx, in) {
			writeXMLError(c, consts.StatusForbidden, "AccessDenied", "bucket policy denies this action")
			c.Abort()
			return
		}

		c.Next(ctx)
	}
}
