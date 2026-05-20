package bucketadmin

import "context"

// RunCreate creates a bucket and writes the result (text or JSON) to opts.Stdout.
func RunCreate(ctx context.Context, opts CreateOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	resp, err := c.Create(ctx, opts.Name, opts.AttachSA, opts.AttachRole)
	if err != nil {
		return err
	}
	if opts.JSONOut {
		return emitJSON(opts.Stdout, resp)
	}
	RenderCreateText(opts.Stdout, opts.Name)
	return nil
}

// RunList prints every bucket on the server.
func RunList(ctx context.Context, opts ListOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	items, err := c.List(ctx)
	if err != nil {
		return err
	}
	if opts.JSONOut {
		return emitJSON(opts.Stdout, items)
	}
	return RenderListText(opts.Stdout, items)
}

// RunInfo prints the bucket detail as a typed tabwriter row.
func RunInfo(ctx context.Context, opts InfoOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	info, err := c.Info(ctx, opts.Name)
	if err != nil {
		return err
	}
	if opts.JSONOut {
		return emitJSON(opts.Stdout, info)
	}
	return RenderInfoText(opts.Stdout, info)
}

// RunDelete removes a bucket; prints the feedback line on success.
func RunDelete(ctx context.Context, opts DeleteOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	if err := c.Delete(ctx, opts.Name, opts.Force, opts.Recursive); err != nil {
		return err
	}
	RenderDeleteText(opts.Stdout, opts.Name)
	return nil
}
