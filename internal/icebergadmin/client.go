package icebergadmin

import (
	"context"
	"strings"

	"github.com/gritive/GrainFS/internal/adminapi"
	"github.com/gritive/GrainFS/internal/iamadmin"
)

// Client speaks to the icebergadmin endpoints on the admin HTTP server.
type Client struct {
	*adminapi.Transport
}

// newClient builds a Client from opts. If opts.Endpoint already looks like
// an http(s) URL (test seam), use it directly.
func newClient(opts BaseOptions) (*Client, error) {
	ep := opts.Endpoint
	if strings.HasPrefix(ep, "http://") || strings.HasPrefix(ep, "https://") {
		tp, _ := adminapi.NewTransport(ep)
		return &Client{Transport: tp}, nil
	}
	resolved, err := iamadmin.ResolveEndpoint(ep)
	if err != nil {
		return nil, err
	}
	tp, err := adminapi.NewTransport(resolved)
	if err != nil {
		return nil, err
	}
	return &Client{Transport: tp}, nil
}

// IcebergConfig calls POST /v1/iceberg/config with the given request body.
func (c *Client) IcebergConfig(ctx context.Context, req adminapi.IcebergConfigRequest) (adminapi.IcebergConfigResponse, error) {
	var resp adminapi.IcebergConfigResponse
	err := c.Post(ctx, "/v1/iceberg/config", req, &resp)
	return resp, err
}
