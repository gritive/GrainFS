package iamadmin

import (
	"context"
	"net/url"

	"github.com/gritive/GrainFS/internal/adminapi"
)

// Client speaks to the iamadmin endpoints on the admin HTTP server.
// Transport plumbing lives in adminapi; this type only wires endpoint methods.
type Client struct {
	*adminapi.Transport
}

// NewClient resolves the endpoint and returns a ready-to-use client.
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

// NewClientForURL builds a Client against an explicit http(s) base URL
// with no auto-discovery. Used by tests against httptest.Server.
func NewClientForURL(rawurl string) *Client {
	tp, _ := adminapi.NewTransport(rawurl)
	return &Client{Transport: tp}
}

// --- ServiceAccount ---

func (c *Client) SACreate(ctx context.Context, name, description string) (SACreateResponse, error) {
	body := map[string]string{"name": name, "description": description}
	var resp SACreateResponse
	err := c.Post(ctx, "/v1/iam/sa", body, &resp)
	return resp, err
}

func (c *Client) SAList(ctx context.Context) ([]SAListItem, error) {
	var resp []SAListItem
	err := c.Get(ctx, "/v1/iam/sa", &resp)
	return resp, err
}

func (c *Client) SAGet(ctx context.Context, saID string) (SAGetResponse, error) {
	var resp SAGetResponse
	err := c.Get(ctx, "/v1/iam/sa/"+url.PathEscape(saID), &resp)
	return resp, err
}

func (c *Client) SADelete(ctx context.Context, saID string) error {
	return c.Delete(ctx, "/v1/iam/sa/"+url.PathEscape(saID), nil)
}

// --- AccessKey ---

// KeyCreateRaw mirrors the existing CLI behavior: the server response body
// is passed through verbatim, both for text and json modes. Returning []byte
// preserves that semantic.
func (c *Client) KeyCreateRaw(ctx context.Context, saID string, buckets []string) ([]byte, error) {
	body := map[string]any{}
	if len(buckets) > 0 {
		body["buckets"] = buckets
	}
	return c.PostRaw(ctx, "/v1/iam/sa/"+url.PathEscape(saID)+"/key", body)
}

func (c *Client) KeyRevoke(ctx context.Context, saID, accessKey string) error {
	return c.Delete(ctx,
		"/v1/iam/sa/"+url.PathEscape(saID)+"/key/"+url.PathEscape(accessKey), nil)
}

// --- Grant ---

func (c *Client) GrantPut(ctx context.Context, saID, bucket, role string) error {
	body := map[string]string{"sa_id": saID, "bucket": bucket, "role": role}
	return c.Put(ctx, "/v1/iam/grant", body, nil)
}

func (c *Client) GrantDelete(ctx context.Context, saID, bucket string) error {
	body := map[string]string{"sa_id": saID, "bucket": bucket}
	return c.Do(ctx, "DELETE", "/v1/iam/grant", body, nil)
}

// GrantListRaw mirrors existing behavior: server body verbatim.
func (c *Client) GrantListRaw(ctx context.Context, saFilter, bucketFilter string) ([]byte, error) {
	path := "/v1/iam/grant"
	q := url.Values{}
	if saFilter != "" {
		q.Set("sa", saFilter)
	}
	if bucketFilter != "" {
		q.Set("bucket", bucketFilter)
	}
	if encoded := q.Encode(); encoded != "" {
		path += "?" + encoded
	}
	return c.GetRaw(ctx, path)
}
