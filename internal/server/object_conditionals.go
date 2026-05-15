package server

import (
	"net/http"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

// checkConditionals evaluates RFC 7232 conditional request headers.
// Returns false and sets response status if the request is short-circuited.
func checkConditionals(c *app.RequestContext, etag string, lastModifiedUnix int64) bool {
	if im := string(c.GetHeader("If-Match")); im != "" {
		if im != "*" && im != etag {
			c.Status(consts.StatusPreconditionFailed)
			return false
		}
	}
	if inm := string(c.GetHeader("If-None-Match")); inm != "" {
		if inm == etag || inm == "*" {
			c.Status(consts.StatusNotModified)
			return false
		}
	}
	if ims := string(c.GetHeader("If-Modified-Since")); ims != "" {
		if t, err := http.ParseTime(ims); err == nil {
			if !time.Unix(lastModifiedUnix, 0).After(t) {
				c.Status(consts.StatusNotModified)
				return false
			}
		}
	}
	if ius := string(c.GetHeader("If-Unmodified-Since")); ius != "" {
		if t, err := http.ParseTime(ius); err == nil {
			if time.Unix(lastModifiedUnix, 0).After(t) {
				c.Status(consts.StatusPreconditionFailed)
				return false
			}
		}
	}
	return true
}
