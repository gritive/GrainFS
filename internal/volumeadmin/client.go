package volumeadmin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/gritive/GrainFS/internal/adminapi"
)

// Client talks to the GrainFS admin Hertz server. The endpoint is a UDS path
// (production; admin server is UDS-only) or an http(s):// URL (test injection).
type Client struct {
	httpClient *http.Client
	baseURL    string
}

// NewClient resolves the endpoint and returns a ready-to-use client.
//
// The endpoint argument is the raw value of --endpoint (may be empty); the
// final endpoint is determined by ResolveEndpoint, walking flag → env.
//
// Transport dispatch on the resolved value:
//   - http(s)://...  — HTTP client (test injection only)
//   - unix:<path>    — UDS dialer (legacy form, still accepted)
//   - <bare path>    — UDS dialer (CLI-facing form)
func NewClient(endpoint string) (*Client, error) {
	ep, err := ResolveEndpoint(endpoint)
	if err != nil {
		return nil, err
	}
	if strings.HasPrefix(ep, "http://") || strings.HasPrefix(ep, "https://") {
		return &Client{
			httpClient: &http.Client{},
			baseURL:    strings.TrimRight(ep, "/"),
		}, nil
	}
	sock := strings.TrimPrefix(ep, "unix:")
	return &Client{
		httpClient: &http.Client{
			Transport: &http.Transport{
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					var d net.Dialer
					return d.DialContext(ctx, "unix", sock)
				},
			},
		},
		baseURL: "http://unix",
	}, nil
}

// NewClientForURL builds a Client targeting an explicit http(s) base URL with
// no auto-discovery. Used by tests against httptest.Server.
func NewClientForURL(rawurl string) *Client {
	return &Client{
		httpClient: &http.Client{},
		baseURL:    strings.TrimRight(rawurl, "/"),
	}
}

// Get issues a GET against path and decodes the JSON response into out (if
// non-nil). Exposed so other admin CLI commands (dashboard, bucket scrub)
// can share the HTTP plumbing without redefining their own client.
func (c *Client) Get(ctx context.Context, path string, out any) error {
	return c.Do(ctx, http.MethodGet, path, nil, out)
}

// Post issues a POST with optional JSON body and decoded JSON response.
func (c *Client) Post(ctx context.Context, path string, in any, out any) error {
	return c.Do(ctx, http.MethodPost, path, in, out)
}

// Delete issues a DELETE and decodes the JSON response if any.
func (c *Client) Delete(ctx context.Context, path string, out any) error {
	return c.Do(ctx, http.MethodDelete, path, nil, out)
}

// Do is the shared HTTP transport. Non-2xx responses surface as *Error so
// callers can errors.As to inspect Code/Status/Details.
func (c *Client) Do(ctx context.Context, method, path string, in any, out any) error {
	var body io.Reader
	if in != nil {
		buf, err := json.Marshal(in)
		if err != nil {
			return fmt.Errorf("marshal request: %w", err)
		}
		body = bytes.NewReader(buf)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		// Map well-known transport-level conditions to the typed *Error
		// envelope so callers can errors.As(err, &*Error) without losing
		// the underlying cause (Unwrap exposes context.Canceled etc).
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return &Error{Code: "transient", Message: "admin request cancelled: " + err.Error(), cause: err}
		}
		var ne *net.OpError
		if errors.As(err, &ne) {
			return &Error{Code: "transient", Message: "admin server unreachable: " + err.Error(), cause: err}
		}
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if out != nil && len(respBody) > 0 {
			if err := json.Unmarshal(respBody, out); err != nil {
				return fmt.Errorf("decode response: %w", err)
			}
		}
		return nil
	}
	var wire adminapi.Error
	cerr := &Error{Status: resp.StatusCode}
	if err := json.Unmarshal(respBody, &wire); err != nil || wire.Code == "" {
		cerr.Code = "internal"
		cerr.Message = string(respBody)
	} else {
		cerr.Code = wire.Code
		cerr.Message = wire.Message
		cerr.Details = wire.Details
	}
	return cerr
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

// DeleteVolume removes a volume. With force=true snapshots are cascaded.
func (c *Client) DeleteVolume(ctx context.Context, name string, force bool) (DeleteResp, error) {
	path := "/v1/volumes/" + url.PathEscape(name)
	if force {
		path += "?force=true"
	}
	var resp DeleteResp
	err := c.Delete(ctx, path, &resp)
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

// CloneVolume creates a fast block-sharing copy of src as dst.
func (c *Client) CloneVolume(ctx context.Context, src, dst string) error {
	return c.Post(ctx, "/v1/volumes/clone", CloneReq{Src: src, Dst: dst}, nil)
}

// RollbackVolume reverts a volume to one of its snapshots.
func (c *Client) RollbackVolume(ctx context.Context, name, snapshotID string) error {
	path := fmt.Sprintf("/v1/volumes/%s/snapshots/%s/rollback", url.PathEscape(name), url.PathEscape(snapshotID))
	return c.Post(ctx, path, nil, nil)
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

// --- Snapshot endpoints ---

// CreateSnapshot creates a snapshot of the named volume.
func (c *Client) CreateSnapshot(ctx context.Context, volume string) (SnapshotCreateResp, error) {
	var resp SnapshotCreateResp
	err := c.Post(ctx, "/v1/volumes/"+url.PathEscape(volume)+"/snapshots", nil, &resp)
	return resp, err
}

// ListSnapshots returns every snapshot of the named volume.
func (c *Client) ListSnapshots(ctx context.Context, volume string) ([]SnapshotInfo, error) {
	var resp []SnapshotInfo
	err := c.Get(ctx, "/v1/volumes/"+url.PathEscape(volume)+"/snapshots", &resp)
	return resp, err
}

// DeleteSnapshot removes one snapshot from a volume.
func (c *Client) DeleteSnapshot(ctx context.Context, volume, snapshotID string) error {
	path := fmt.Sprintf("/v1/volumes/%s/snapshots/%s", url.PathEscape(volume), url.PathEscape(snapshotID))
	return c.Delete(ctx, path, nil)
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
