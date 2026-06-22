package admincli

import (
	"context"
	"net/url"

	"github.com/gritive/GrainFS/internal/adminapi"
)

// Client speaks to the admincli endpoints on the admin HTTP server.
// Transport plumbing lives in adminapi; this type only wires endpoint methods.
type Client struct {
	*adminapi.Transport
}

// NewClient resolves the endpoint and returns a ready-to-use client.
//
// The endpoint argument is the raw value of --endpoint (may be empty); the
// final endpoint is determined by ResolveEndpoint, walking flag → env.
func NewClient(endpoint string) (*Client, error) {
	ep, err := ResolveEndpoint(endpoint)
	if err != nil {
		return nil, err
	}
	tp, err := adminapi.NewTransport(ep)
	if err != nil {
		return nil, err
	}
	return &Client{Transport: tp}, nil
}

// NewClientForURL builds a Client against an explicit http(s) base URL with
// no auto-discovery. Used by tests against httptest.Server.
func NewClientForURL(rawurl string) *Client {
	tp, _ := adminapi.NewTransport(rawurl)
	return &Client{Transport: tp}
}

// --- Scrub endpoints ---

// GetScrubJob returns the current state of a scrub session.
func (c *Client) GetScrubJob(ctx context.Context, sessionID string) (ScrubJobInfo, error) {
	var resp ScrubJobInfo
	err := c.Get(ctx, "/v1/scrub/jobs/"+url.PathEscape(sessionID), &resp)
	return resp, err
}

// ListScrubJobs returns every active or recently-finished scrub session.
func (c *Client) ListScrubJobs(ctx context.Context) (ListScrubJobsResp, error) {
	var resp ListScrubJobsResp
	err := c.Get(ctx, "/v1/scrub/jobs", &resp)
	return resp, err
}
