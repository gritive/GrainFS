package credentialadmin

import (
	"context"
	"net/url"

	"github.com/gritive/GrainFS/internal/adminapi"
)

type Client struct {
	*adminapi.Transport
}

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

func NewClientForURL(rawurl string) *Client {
	tp, _ := adminapi.NewTransport(rawurl)
	return &Client{Transport: tp}
}

func (c *Client) Create(ctx context.Context, req CreateReq) (Credential, error) {
	var resp Credential
	err := c.Post(ctx, "/v1/credentials", req, &resp)
	return resp, err
}

func (c *Client) List(ctx context.Context, saID, protocol string) (ListResp, error) {
	return c.ListFiltered(ctx, saID, protocol, "")
}

func (c *Client) ListFiltered(ctx context.Context, saID, protocol, resource string) (ListResp, error) {
	q := url.Values{}
	if saID != "" {
		q.Set("sa_id", saID)
	}
	if protocol != "" {
		q.Set("protocol", protocol)
	}
	if resource != "" {
		q.Set("resource", resource)
	}
	path := "/v1/credentials"
	if encoded := q.Encode(); encoded != "" {
		path += "?" + encoded
	}
	var resp ListResp
	err := c.Get(ctx, path, &resp)
	return resp, err
}

func (c *Client) GetCredential(ctx context.Context, id string) (Credential, error) {
	var resp Credential
	err := c.Get(ctx, "/v1/credentials/"+url.PathEscape(id), &resp)
	return resp, err
}

func (c *Client) Rotate(ctx context.Context, id string) (Credential, error) {
	var resp Credential
	err := c.Post(ctx, "/v1/credentials/"+url.PathEscape(id)+"/rotate", nil, &resp)
	return resp, err
}

func (c *Client) Revoke(ctx context.Context, id string) (RevokeResp, error) {
	var resp RevokeResp
	err := c.Delete(ctx, "/v1/credentials/"+url.PathEscape(id), &resp)
	return resp, err
}
