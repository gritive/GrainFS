package bucketadmin

import "context"

// RunUpstreamPut writes (or upserts) the upstream credentials for a bucket.
// The legacy CLI prints no output on success.
func RunUpstreamPut(ctx context.Context, opts UpstreamPutOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	return c.UpstreamPut(ctx, opts)
}

// RunUpstreamGet prints the bucket's upstream credentials document verbatim
// (both --text and --json modes).
func RunUpstreamGet(ctx context.Context, opts UpstreamGetOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	raw, err := c.UpstreamGetRaw(ctx, opts.Bucket)
	if err != nil {
		return err
	}
	WriteRawWithNewline(opts.Stdout, raw)
	return nil
}

// RunUpstreamList prints the server's list of upstream-configured buckets
// verbatim.
func RunUpstreamList(ctx context.Context, opts UpstreamListOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	raw, err := c.UpstreamListRaw(ctx)
	if err != nil {
		return err
	}
	WriteRawWithNewline(opts.Stdout, raw)
	return nil
}

// RunUpstreamDelete removes the upstream credentials for a bucket.
// The legacy CLI prints no output on success.
func RunUpstreamDelete(ctx context.Context, opts UpstreamDeleteOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	return c.UpstreamDelete(ctx, opts.Bucket)
}
