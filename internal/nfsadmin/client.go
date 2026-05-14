package nfsadmin

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

func (c *Client) UpsertExport(ctx context.Context, req NfsExportUpsertReq) (NfsExportInfo, error) {
	var resp NfsExportInfo
	err := c.Post(ctx, "/v1/nfs/exports", req, &resp)
	return resp, err
}

func (c *Client) UpdateExport(ctx context.Context, bucket string, req NfsExportUpsertReq) (NfsExportInfo, error) {
	var resp NfsExportInfo
	err := c.Patch(ctx, "/v1/nfs/exports/"+url.PathEscape(bucket), req, &resp)
	return resp, err
}

func (c *Client) GetExport(ctx context.Context, bucket string) (NfsExportInfo, error) {
	var resp NfsExportInfo
	err := c.Get(ctx, "/v1/nfs/exports/"+url.PathEscape(bucket), &resp)
	return resp, err
}

func (c *Client) ListExports(ctx context.Context) (ListNfsExportsResp, error) {
	var resp ListNfsExportsResp
	err := c.Get(ctx, "/v1/nfs/exports", &resp)
	return resp, err
}

func (c *Client) DeleteExport(ctx context.Context, bucket string) error {
	return c.Delete(ctx, "/v1/nfs/exports/"+url.PathEscape(bucket), nil)
}
