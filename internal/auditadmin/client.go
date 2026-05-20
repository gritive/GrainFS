package auditadmin

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/gritive/GrainFS/internal/adminapi"
)

// Client speaks to the auditadmin endpoints on the admin HTTP server.
type Client struct {
	*adminapi.Transport
}

// NewClient resolves the endpoint (flag value → GRAINFS_ADMIN_SOCKET env →
// fail-fast) and returns a ready-to-use client.
func NewClient(endpoint string) (*Client, error) {
	ep, err := resolveEndpoint(endpoint)
	if err != nil {
		return nil, err
	}
	tp, err := adminapi.NewTransport(ep)
	if err != nil {
		return nil, err
	}
	return &Client{Transport: tp}, nil
}

// NewClientForURL builds a Client against an explicit http(s) base URL
// with no auto-discovery. Used by tests against httptest.Server.
func NewClientForURL(rawurl string) *Client {
	tp, _ := adminapi.NewTransport(rawurl)
	return &Client{Transport: tp}
}

// resolveEndpoint mirrors iamadmin.ResolveEndpoint: flag value →
// GRAINFS_ADMIN_SOCKET env → fail-fast.
func resolveEndpoint(raw string) (string, error) {
	ep := strings.TrimSpace(raw)
	if ep == "" {
		ep = strings.TrimSpace(os.Getenv("GRAINFS_ADMIN_SOCKET"))
	}
	if ep == "" {
		return "", fmt.Errorf("admin endpoint not configured.\n" +
			"  Hint: set GRAINFS_ADMIN_SOCKET=<data-dir>/admin.sock or use --endpoint")
	}
	if strings.HasPrefix(ep, "http://") || strings.HasPrefix(ep, "https://") {
		return "", fmt.Errorf("admin endpoint must be a UDS socket path; got %q.\n"+
			"  Use the admin socket: --endpoint <data-dir>/admin.sock", ep)
	}
	return strings.TrimPrefix(ep, "unix:"), nil
}

// dialClient builds a Client from opts. If opts.Endpoint already looks like
// an http(s) URL (test seam), bypass resolveEndpoint so tests against
// httptest.Server work without environment setup.
func dialClient(opts BaseOptions) (*Client, error) {
	if strings.HasPrefix(opts.Endpoint, "http://") || strings.HasPrefix(opts.Endpoint, "https://") {
		return NewClientForURL(opts.Endpoint), nil
	}
	return NewClient(opts.Endpoint)
}

// QueryAudit posts a raw SQL query to /v1/audit/query.
func (c *Client) QueryAudit(ctx context.Context, req QueryRequest) (QueryResponse, error) {
	var resp QueryResponse
	err := c.Post(ctx, "/v1/audit/query", req, &resp)
	return resp, err
}

// RecentDenies returns the most-recent deny-outcome audit rows.
func (c *Client) RecentDenies(ctx context.Context, limit int) (QueryResponse, error) {
	path := "/v1/audit/recent-denies"
	if limit > 0 {
		path += "?limit=" + url.QueryEscape(fmt.Sprintf("%d", limit))
	}
	var resp QueryResponse
	err := c.Get(ctx, path, &resp)
	return resp, err
}

// BySA returns audit rows where sa_id equals said.
func (c *Client) BySA(ctx context.Context, said string, limit int) (QueryResponse, error) {
	path := "/v1/audit/by-sa/" + url.PathEscape(said)
	if limit > 0 {
		path += "?limit=" + url.QueryEscape(fmt.Sprintf("%d", limit))
	}
	var resp QueryResponse
	err := c.Get(ctx, path, &resp)
	return resp, err
}

// ByRequestID returns audit rows where request_id equals rid.
func (c *Client) ByRequestID(ctx context.Context, rid string) (QueryResponse, error) {
	var resp QueryResponse
	err := c.Get(ctx, "/v1/audit/by-request-id/"+url.PathEscape(rid), &resp)
	return resp, err
}
