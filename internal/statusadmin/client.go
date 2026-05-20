// Package statusadmin is the admin-plane client for the grainfs status command.
// Consumed by the CLI (cmd/grainfs) and tests; the server side lives in
// internal/server/admin.
package statusadmin

import (
	"context"
	"os"
	"strings"

	"github.com/gritive/GrainFS/internal/adminapi"
)

// Client speaks to the status endpoint on the admin HTTP server.
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

func resolveEndpoint(raw string) (string, error) {
	ep := strings.TrimSpace(raw)
	if ep == "" {
		ep = strings.TrimSpace(os.Getenv("GRAINFS_ADMIN_SOCKET"))
	}
	if ep == "" {
		return "", errEndpointNotConfigured
	}
	if strings.HasPrefix(ep, "http://") || strings.HasPrefix(ep, "https://") {
		return "", errEndpointMustBeUDS
	}
	return strings.TrimPrefix(ep, "unix:"), nil
}

func dialClient(opts BaseOptions) (*Client, error) {
	if strings.HasPrefix(opts.Endpoint, "http://") || strings.HasPrefix(opts.Endpoint, "https://") {
		return NewClientForURL(opts.Endpoint), nil
	}
	return NewClient(opts.Endpoint)
}

// GetStatus fetches the current status report from GET /v1/status.
func (c *Client) GetStatus(ctx context.Context) (adminapi.StatusReport, error) {
	var resp adminapi.StatusReport
	err := c.Get(ctx, "/v1/status", &resp)
	return resp, err
}
