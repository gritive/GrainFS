package iamadmin

import "context"

// RunGrantPut grants `role` on `bucket` to the named ServiceAccount.
// role is one of Read|Write|Admin. The server returns no body on success;
// the legacy CLI prints nothing.
func RunGrantPut(ctx context.Context, opts GrantPutOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	return c.GrantPut(ctx, opts.SAID, opts.Bucket, opts.Role)
}

// RunGrantDelete revokes the SA's grant on the named bucket.
func RunGrantDelete(ctx context.Context, opts GrantDeleteOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	return c.GrantDelete(ctx, opts.SAID, opts.Bucket)
}

// RunGrantList prints the server's grant list verbatim. Filters --sa /
// --bucket map to query parameters; either, both, or neither may be set.
func RunGrantList(ctx context.Context, opts GrantListOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	raw, err := c.GrantListRaw(ctx, opts.SAFilter, opts.BucketFilter)
	if err != nil {
		return err
	}
	WriteRawWithNewline(opts.Stdout, raw)
	return nil
}
