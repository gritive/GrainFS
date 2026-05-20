package admin

import (
	"context"
	"fmt"

	"github.com/gritive/GrainFS/internal/adminapi"
)

// IcebergConfig resolves the SA's first active AccessKey and builds the
// client-agnostic OAuth2 connection bundle for Iceberg REST Catalog clients.
//
// When req.NoReveal is true the server zeroes ClientSecret before returning
// so the wire never carries the plaintext secret (defense-in-depth).
func IcebergConfig(ctx context.Context, d *Deps, req adminapi.IcebergConfigRequest) (adminapi.IcebergConfigResponse, error) {
	if d.IcebergConfig == nil {
		return adminapi.IcebergConfigResponse{}, NewUnavailable("iceberg config admin not available")
	}
	if req.Warehouse == "" {
		return adminapi.IcebergConfigResponse{}, NewInvalid("warehouse is required")
	}
	if req.SAID == "" {
		return adminapi.IcebergConfigResponse{}, NewInvalid("sa is required")
	}

	ak, sk, err := d.IcebergConfig.RevealSAKeyPair(ctx, req.SAID)
	if err != nil {
		return adminapi.IcebergConfigResponse{}, err
	}

	base := d.PublicURL
	if base == "" {
		base = "http://localhost:9000"
	}

	resp := adminapi.IcebergConfigResponse{
		CatalogURI:    fmt.Sprintf("%s/iceberg", base),
		OAuthTokenURI: fmt.Sprintf("%s/iceberg/v1/oauth/tokens", base),
		Warehouse:     req.Warehouse,
		ClientID:      ak,
		ClientSecret:  sk,
	}
	if req.NoReveal {
		resp.ClientSecret = ""
	}
	return resp, nil
}
