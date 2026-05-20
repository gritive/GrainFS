// Package bucketadmin client. See file-level doc in types.go.
package bucketadmin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"

	"github.com/gritive/GrainFS/internal/adminapi"
)

// Client speaks to the bucketadmin endpoints on the admin HTTP server.
// Transport plumbing lives in adminapi.
type Client struct {
	*adminapi.Transport
}

// NewClient resolves the endpoint (flag → GRAINFS_ADMIN_SOCKET env →
// fail-fast) and returns a ready-to-use client. Same resolution order as
// the legacy bucket CLI.
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

// NewClientForURL builds a Client against an explicit http(s) URL — test seam.
func NewClientForURL(rawurl string) *Client {
	tp, _ := adminapi.NewTransport(rawurl)
	return &Client{Transport: tp}
}

// --- Bucket lifecycle ---

// Create makes a new bucket; attach is nil-safe (no SA/policy attached).
func (c *Client) Create(ctx context.Context, name, attachSA, attachRole string) (CreateResponse, error) {
	body := map[string]any{"name": name}
	if attachSA != "" || attachRole != "" {
		body["attach"] = map[string]string{"sa": attachSA, "policy": attachRole}
	}
	var resp CreateResponse
	err := c.Post(ctx, "/v1/buckets", body, &resp)
	return resp, err
}

// List returns all buckets known to the server.
func (c *Client) List(ctx context.Context) ([]ListItem, error) {
	var resp []ListItem
	err := c.Get(ctx, "/v1/buckets", &resp)
	return resp, err
}

// InfoRaw returns the server's detail document verbatim so operator-facing
// output is unchanged.
func (c *Client) InfoRaw(ctx context.Context, name string) ([]byte, error) {
	return c.GetRaw(ctx, "/v1/buckets/"+url.PathEscape(name))
}

// Delete removes a bucket; force and recursive map to query params.
func (c *Client) Delete(ctx context.Context, name string, force, recursive bool) error {
	q := url.Values{}
	if force {
		q.Set("force", "true")
	}
	if recursive {
		q.Set("recursive", "true")
	}
	path := "/v1/buckets/" + url.PathEscape(name)
	if encoded := q.Encode(); encoded != "" {
		path += "?" + encoded
	}
	return c.Transport.Delete(ctx, path, nil)
}

// --- Upstream credentials ---

func (c *Client) UpstreamPut(ctx context.Context, opts UpstreamPutOptions) error {
	body := map[string]string{
		"bucket":        opts.Bucket,
		"scheme":        opts.Scheme,
		"endpoint":      opts.Endpoint,
		"access_key":    opts.AccessKey,
		"secret_key":    opts.SecretKey,
		"region":        opts.Region,
		"remote_bucket": opts.RemoteBucket,
	}
	return c.Put(ctx, "/v1/upstreams", body, nil)
}

func (c *Client) UpstreamGetRaw(ctx context.Context, bucket string) ([]byte, error) {
	return c.GetRaw(ctx, "/v1/upstreams/"+url.PathEscape(bucket))
}

func (c *Client) UpstreamListRaw(ctx context.Context) ([]byte, error) {
	return c.GetRaw(ctx, "/v1/upstreams")
}

func (c *Client) UpstreamDelete(ctx context.Context, bucket string) error {
	return c.Transport.Delete(ctx, "/v1/upstreams/"+url.PathEscape(bucket), nil)
}

// --- Policy ---

func (c *Client) PolicyGetRaw(ctx context.Context, bucket string) ([]byte, error) {
	return c.GetRaw(ctx, "/v1/buckets/"+url.PathEscape(bucket)+"/policy")
}

// PolicySet sends the policy document bytes verbatim. Server is the
// authority on validation.
func (c *Client) PolicySet(ctx context.Context, bucket string, policy []byte) error {
	return c.Put(ctx, "/v1/buckets/"+url.PathEscape(bucket)+"/policy",
		json.RawMessage(policy), nil)
}

func (c *Client) PolicyDelete(ctx context.Context, bucket string) error {
	return c.Transport.Delete(ctx, "/v1/buckets/"+url.PathEscape(bucket)+"/policy", nil)
}

// --- Versioning ---

func (c *Client) VersioningGet(ctx context.Context, bucket string) (VersioningStatus, error) {
	var resp VersioningStatus
	err := c.Get(ctx, "/v1/buckets/"+url.PathEscape(bucket)+"/versioning", &resp)
	return resp, err
}

func (c *Client) VersioningEnable(ctx context.Context, bucket string) error {
	return c.Put(ctx, "/v1/buckets/"+url.PathEscape(bucket)+"/versioning",
		map[string]string{"status": "Enabled"}, nil)
}

func (c *Client) VersioningSuspend(ctx context.Context, bucket string) error {
	return c.Put(ctx, "/v1/buckets/"+url.PathEscape(bucket)+"/versioning",
		map[string]string{"status": "Suspended"}, nil)
}

// avoid unused imports under partial compilation
var _ = http.MethodDelete
