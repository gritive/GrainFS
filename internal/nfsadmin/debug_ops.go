package nfsadmin

import (
	"context"
	"encoding/json"
)

func RunDebug(ctx context.Context, opts DebugExportOptions) error {
	c, err := clientFor(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.BaseOptions)
	defer cancel()
	resp, err := c.ExportDebug(ctx, opts.Bucket)
	if err != nil {
		if !opts.JSONOut {
			RenderError(stderr(opts.BaseOptions), err)
		}
		return err
	}
	if opts.JSONOut {
		enc := json.NewEncoder(stdout(opts.BaseOptions))
		enc.SetIndent("", "  ")
		return enc.Encode(resp)
	}
	return RenderExportDebug(stdout(opts.BaseOptions), resp)
}
