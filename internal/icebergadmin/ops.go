package icebergadmin

import (
	"context"
	"encoding/json"
	"fmt"
	"text/tabwriter"
	"time"

	"github.com/gritive/GrainFS/internal/adminapi"
)

// withTimeout returns ctx unchanged if d <= 0, otherwise applies a deadline.
func withTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if d <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, d)
}

// RunIcebergConfig calls the admin server to retrieve the iceberg OAuth bundle
// and writes the result to opts.Stdout. Default mode prints a tabwriter table;
// --json emits the raw JSON response.
func RunIcebergConfig(ctx context.Context, opts IcebergConfigOptions) error {
	c, err := newClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()

	req := adminapi.IcebergConfigRequest{
		Warehouse: opts.Warehouse,
		SAID:      opts.SAID,
		NoReveal:  opts.NoReveal,
	}
	resp, err := c.IcebergConfig(ctx, req)
	if err != nil {
		return err
	}

	if opts.JSONOut {
		b, merr := json.Marshal(resp)
		if merr != nil {
			return fmt.Errorf("marshal: %w", merr)
		}
		_, err = fmt.Fprintln(opts.Stdout, string(b))
		return err
	}

	secretDisplay := resp.ClientSecret
	if opts.NoReveal {
		secretDisplay = "<hidden — re-run without --no-reveal to print>"
	}

	tw := tabwriter.NewWriter(opts.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "catalog_uri:\t%s\n", resp.CatalogURI)
	fmt.Fprintf(tw, "oauth_token_uri:\t%s\n", resp.OAuthTokenURI)
	fmt.Fprintf(tw, "warehouse:\t%s\n", resp.Warehouse)
	fmt.Fprintf(tw, "client_id:\t%s\n", resp.ClientID)
	fmt.Fprintf(tw, "client_secret:\t%s\n", secretDisplay)
	return tw.Flush()
}
