package serveruntime

import (
	"bytes"
	"context"
	"net/http"

	"github.com/cloudwego/hertz/pkg/app"
)

func wrapStdlibNoParam(fn func(http.ResponseWriter, *http.Request)) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		req := buildHTTPRequest(ctx, c)
		rw := newHertzResponseWriter()
		fn(rw, req)
		rw.flushTo(c)
	}
}

func wrapStdlibOneParam(p string, fn func(http.ResponseWriter, *http.Request, string)) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		req := buildHTTPRequest(ctx, c)
		rw := newHertzResponseWriter()
		fn(rw, req, c.Param(p))
		rw.flushTo(c)
	}
}

func wrapStdlibTwoParams(p1, p2 string, fn func(http.ResponseWriter, *http.Request, string, string)) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		req := buildHTTPRequest(ctx, c)
		rw := newHertzResponseWriter()
		fn(rw, req, c.Param(p1), c.Param(p2))
		rw.flushTo(c)
	}
}

// buildHTTPRequest constructs a stdlib *http.Request from a Hertz
// RequestContext, preserving method, path, query, headers, body, and ctx.
// Path+query is stored in the request URL; admin handlers read it via
// r.URL.Query(). Headers come from Hertz's request header.
func buildHTTPRequest(ctx context.Context, c *app.RequestContext) *http.Request {
	method := string(c.Method())
	uri := c.Request.URI()
	target := string(uri.Path())
	if qs := uri.QueryString(); len(qs) > 0 {
		target += "?" + string(qs)
	}
	body := c.Request.Body()
	r, _ := http.NewRequestWithContext(ctx, method, target, bytes.NewReader(body))
	c.Request.Header.VisitAll(func(k, v []byte) {
		r.Header.Add(string(k), string(v))
	})
	return r
}

// hertzResponseWriter is a minimal http.ResponseWriter that captures the
// stdlib handler's status, headers, and body, then flushes them onto a
// Hertz RequestContext via flushTo.
type hertzResponseWriter struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func newHertzResponseWriter() *hertzResponseWriter {
	return &hertzResponseWriter{header: http.Header{}, status: http.StatusOK}
}

func (h *hertzResponseWriter) Header() http.Header { return h.header }

func (h *hertzResponseWriter) Write(p []byte) (int, error) { return h.body.Write(p) }

func (h *hertzResponseWriter) WriteHeader(code int) { h.status = code }

func (h *hertzResponseWriter) flushTo(c *app.RequestContext) {
	for k, vs := range h.header {
		for _, v := range vs {
			c.Response.Header.Add(k, v)
		}
	}
	c.SetStatusCode(h.status)
	if h.body.Len() > 0 {
		c.Response.SetBody(h.body.Bytes())
	}
}
