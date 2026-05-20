package iamadmin

import "context"

// RunKeyCreate issues a new AccessKey for the named ServiceAccount. The
// server response body (containing the one-time secret) is printed
// verbatim — both --json and text modes produce the same output, matching
// the legacy CLI semantic.
func RunKeyCreate(ctx context.Context, opts KeyCreateOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	raw, err := c.KeyCreateRaw(ctx, opts.SAID, opts.Buckets)
	if err != nil {
		return err
	}
	WriteRawWithNewline(opts.Stdout, raw)
	return nil
}

// RunKeyRevoke deletes a single AccessKey. The legacy CLI prints nothing
// on success; this matches that behavior.
func RunKeyRevoke(ctx context.Context, opts KeyRevokeOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	return c.KeyRevoke(ctx, opts.SAID, opts.AccessKey)
}
