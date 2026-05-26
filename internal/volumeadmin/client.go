package volumeadmin

import (
	"context"
	"fmt"
	"net/url"

	"github.com/gritive/GrainFS/internal/adminapi"
)

// Client speaks to the volumeadmin endpoints on the admin HTTP server.
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

// --- Typed endpoints ---

// ListVolumes returns every volume known to the admin server.
func (c *Client) ListVolumes(ctx context.Context) (ListVolumesResp, error) {
	var resp ListVolumesResp
	err := c.Get(ctx, "/v1/volumes", &resp)
	return resp, err
}

// CreateVolume creates a new volume of the given size.
func (c *Client) CreateVolume(ctx context.Context, req CreateVolumeReq) (VolumeInfo, error) {
	var resp VolumeInfo
	err := c.Post(ctx, "/v1/volumes", req, &resp)
	return resp, err
}

// GetVolume returns metadata for one volume.
func (c *Client) GetVolume(ctx context.Context, name string) (VolumeInfo, error) {
	var resp VolumeInfo
	err := c.Get(ctx, "/v1/volumes/"+url.PathEscape(name), &resp)
	return resp, err
}

// StatVolume returns volume metadata together with recent incidents.
func (c *Client) StatVolume(ctx context.Context, name string) (VolumeStatResp, error) {
	var resp VolumeStatResp
	err := c.Get(ctx, "/v1/volumes/"+url.PathEscape(name)+"/stat", &resp)
	return resp, err
}

// DeleteVolume removes a volume.
func (c *Client) DeleteVolume(ctx context.Context, name string) (DeleteResp, error) {
	var resp DeleteResp
	err := c.Delete(ctx, "/v1/volumes/"+url.PathEscape(name), &resp)
	return resp, err
}

// ResizeVolume grows a volume; shrink returns the typed "unsupported" error.
func (c *Client) ResizeVolume(ctx context.Context, name string, size int64) (ResizeResp, error) {
	var resp ResizeResp
	err := c.Post(ctx, "/v1/volumes/"+url.PathEscape(name)+"/resize", ResizeReq{Size: size}, &resp)
	return resp, err
}

// RecalculateVolume re-counts AllocatedBlocks against actual block objects.
func (c *Client) RecalculateVolume(ctx context.Context, name string) (RecalculateResp, error) {
	var resp RecalculateResp
	err := c.Post(ctx, "/v1/volumes/"+url.PathEscape(name)+"/recalculate", nil, &resp)
	return resp, err
}

// WriteAtVolume writes raw bytes at an offset (debug/test helper).
func (c *Client) WriteAtVolume(ctx context.Context, name string, offset int64, data []byte) (WriteAtResp, error) {
	var resp WriteAtResp
	err := c.Post(ctx, "/v1/volumes/"+url.PathEscape(name)+"/write-at",
		WriteAtReq{Name: name, Offset: offset, Data: data}, &resp)
	return resp, err
}

// ReadAtVolume reads raw bytes at an offset (debug/test helper).
func (c *Client) ReadAtVolume(ctx context.Context, name string, offset, length int64) (ReadAtResp, error) {
	var resp ReadAtResp
	err := c.Post(ctx, "/v1/volumes/"+url.PathEscape(name)+"/read-at",
		ReadAtReq{Name: name, Offset: offset, Length: length}, &resp)
	return resp, err
}

// --- Scrub endpoints ---

// TriggerScrub starts (or rejoins) a scrub session over a single volume.
func (c *Client) TriggerScrub(ctx context.Context, req ScrubTriggerReq) (ScrubTriggerResp, error) {
	var resp ScrubTriggerResp
	err := c.Post(ctx, fmt.Sprintf("/v1/volumes/%s/scrub", url.PathEscape(req.Name)), req, &resp)
	return resp, err
}

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

// CancelScrub cancels a running scrub session.
func (c *Client) CancelScrub(ctx context.Context, sessionID string) error {
	return c.Delete(ctx, "/v1/scrub/jobs/"+url.PathEscape(sessionID), nil)
}
