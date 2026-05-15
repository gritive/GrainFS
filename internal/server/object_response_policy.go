package server

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/cloudwego/hertz/pkg/app"

	"github.com/gritive/GrainFS/internal/storage"
)

func (s *Server) writeObjectCachePolicy(c *app.RequestContext) {
	if s.verifier != nil {
		c.Header("Cache-Control", "private, no-store")
		return
	}
	c.Header("Cache-Control", "public, max-age=3600")
}

func (s *Server) writeObjectReadHeaders(c *app.RequestContext, obj *storage.Object, includeLength bool) string {
	etag := fmt.Sprintf("\"%s\"", obj.ETag)
	c.Header("Content-Type", obj.ContentType)
	if includeLength {
		c.Header("Content-Length", strconv.FormatInt(obj.Size, 10))
	}
	c.Header("ETag", etag)
	c.Header("Last-Modified", time.Unix(obj.LastModified, 0).UTC().Format(http.TimeFormat))
	c.Header("Accept-Ranges", "bytes")
	writeUserMetadataHeaders(c, obj.UserMetadata)
	if obj.VersionID != "" {
		c.Header("X-Amz-Version-Id", obj.VersionID)
	}
	s.writeObjectCachePolicy(c)
	return etag
}
