package server

import (
	"net/http"
	"net/url"

	"github.com/cloudwego/hertz/pkg/app"
)

// hertzResponseWriter adapts Hertz RequestContext to http.ResponseWriter for stdlib handlers.
type hertzResponseWriter struct {
	c          *app.RequestContext
	header     http.Header
	statusCode int
	written    bool
}

func newResponseWriter(c *app.RequestContext) *hertzResponseWriter {
	return &hertzResponseWriter{c: c, header: make(http.Header), statusCode: http.StatusOK}
}

func (w *hertzResponseWriter) Header() http.Header {
	return w.header
}

func (w *hertzResponseWriter) Write(data []byte) (int, error) {
	if !w.written {
		// Flush accumulated headers to Hertz response
		for k, vs := range w.header {
			for _, v := range vs {
				w.c.Response.Header.Set(k, v)
			}
		}
		w.c.SetStatusCode(w.statusCode)
		w.written = true
	}
	return w.c.Write(data)
}

func (w *hertzResponseWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	if !w.written {
		for k, vs := range w.header {
			for _, v := range vs {
				w.c.Response.Header.Set(k, v)
			}
		}
		w.c.SetStatusCode(statusCode)
		w.written = true
	}
}

// toHTTPRequest converts Hertz RequestContext to a stdlib http.Request for SigV4 verification.
func toHTTPRequest(c *app.RequestContext) *http.Request {
	u := &url.URL{
		Path:     string(c.URI().Path()),
		RawQuery: string(c.URI().QueryString()),
	}
	r := &http.Request{
		Method: string(c.Method()),
		Host:   string(c.Host()),
		URL:    u,
		Header: make(http.Header),
	}

	c.Request.Header.VisitAll(func(key, value []byte) {
		r.Header.Set(string(key), string(value))
	})
	return r
}
