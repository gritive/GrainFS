package server

import (
	"context"
	"net"
	"strings"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

func isBucketFormPost(c *app.RequestContext) bool {
	return string(c.Method()) == "POST" &&
		strings.TrimPrefix(c.Param("key"), "/") == "" &&
		strings.HasPrefix(string(c.GetHeader("Content-Type")), "multipart/form-data")
}

func s3PathBucketKey(path string) (string, string) {
	if !routeIsS3Path(path) {
		return "", ""
	}
	path = strings.TrimPrefix(path, "/")
	if path == "" {
		return "", ""
	}
	bucket, key, _ := strings.Cut(path, "/")
	return bucket, key
}

func (s *Server) authMiddleware() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		path := string(c.URI().Path())
		switch routeAuthnPolicyForPath(path) {
		case routeAuthnAnonymous:
			c.Next(ctx)
			return
		case routeAuthnLocalhost:
			if isLocalhostAddr(c.RemoteAddr().String()) {
				c.Next(ctx)
				return
			}
		}

		r := toHTTPRequest(c)
		if isBucketFormPost(c) {
			ctx = WithAccessKey(ctx, "")
			c.Next(ctx)
			return
		}

		// Anonymous fast path: unsigned GET/HEAD on plain object data may be allowed
		// by object ACL. Subresource requests (?acl, ?tagging, etc.) and all other
		// methods always require authentication, checked via RawQuery == "".
		method := string(c.Method())
		key := strings.TrimPrefix(c.Param("key"), "/")
		bucket := c.Param("bucket")
		if bucket == "" {
			bucket, key = s3PathBucketKey(path)
		}
		if nextCtx, ok := s.authenticateAuditInternalRequest(ctx, c, r, bucket, key, method); ok {
			ctx = nextCtx
			c.Next(ctx)
			return
		}
		isObjectRead := (method == "GET" || method == "HEAD") && key != "" && r.URL.RawQuery == ""
		if isObjectRead && r.Header.Get("Authorization") == "" {
			ctx = WithAccessKey(ctx, "")
			c.Next(ctx)
			return
		}

		nextCtx, failure := s.authenticateSignedRequest(ctx, r)
		if failure != nil {
			s.recordAuditAuthFailure(ctx, c, failure.status, failure.reason)
			writeXMLError(c, failure.status, failure.code, failure.message)
			c.Abort()
			return
		}
		ctx = nextCtx
		c.Next(ctx)
	}
}

// isLocalhostAddr reports whether addr (typically from c.RemoteAddr().String(),
// in "host:port" or "[host]:port" form) is a loopback address.
// Covers IPv4 loopback, IPv6 loopback, the IPv4-mapped IPv6 loopback
// ([::ffff:127.0.0.1]:PORT), and the literal "localhost" hostname.
func isLocalhostAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr // no port present
	}
	switch host {
	case "127.0.0.1", "::1", "::ffff:127.0.0.1", "localhost":
		return true
	}
	return false
}

// localhostOnly returns a middleware that rejects non-localhost connections with 403.
func localhostOnly() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		if !isLocalhostAddr(c.RemoteAddr().String()) {
			c.JSON(consts.StatusForbidden, map[string]string{
				"error": "admin endpoints are restricted to localhost",
			})
			c.Abort()
			return
		}
		c.Next(ctx)
	}
}
