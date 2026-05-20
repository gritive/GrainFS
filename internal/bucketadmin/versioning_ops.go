package bucketadmin

import "context"

// RunVersioningGet prints the bucket's versioning status. Text mode renders
// a tabwriter row (empty status → "Off"); JSON mode emits the typed struct.
func RunVersioningGet(ctx context.Context, opts VersioningGetOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	status, err := c.VersioningGet(ctx, opts.Bucket)
	if err != nil {
		return err
	}
	if opts.JSONOut {
		return emitJSON(opts.Stdout, status)
	}
	return RenderVersioningGetText(opts.Stdout, opts.Bucket, status)
}

// RunVersioningEnable marks the bucket as versioning-enabled.
func RunVersioningEnable(ctx context.Context, opts VersioningEnableOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	if err := c.VersioningEnable(ctx, opts.Bucket); err != nil {
		return err
	}
	RenderVersioningEnableText(opts.Stdout, opts.Bucket)
	return nil
}

// RunVersioningSuspend marks the bucket as versioning-suspended.
func RunVersioningSuspend(ctx context.Context, opts VersioningSuspendOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	if err := c.VersioningSuspend(ctx, opts.Bucket); err != nil {
		return err
	}
	RenderVersioningSuspendText(opts.Stdout, opts.Bucket)
	return nil
}
