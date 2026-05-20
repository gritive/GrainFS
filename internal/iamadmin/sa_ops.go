package iamadmin

import "context"

// RunSACreate creates a ServiceAccount and writes the result (text or JSON)
// to opts.Stdout. Behavior matches the legacy `grainfs iam sa create` CLI.
func RunSACreate(ctx context.Context, opts SACreateOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	resp, err := c.SACreate(ctx, opts.Name, opts.Description)
	if err != nil {
		return err
	}
	if opts.JSONOut {
		return emitJSON(opts.Stdout, resp)
	}
	RenderSACreateText(opts.Stdout, resp)
	return nil
}

// RunSAList prints all ServiceAccounts known to the admin server.
func RunSAList(ctx context.Context, opts SAListOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	resp, err := c.SAList(ctx)
	if err != nil {
		return err
	}
	if opts.JSONOut {
		return emitJSON(opts.Stdout, resp)
	}
	RenderSAListText(opts.Stdout, resp)
	return nil
}

// RunSAGet prints detail for one ServiceAccount.
func RunSAGet(ctx context.Context, opts SAGetOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	resp, err := c.SAGet(ctx, opts.SAID)
	if err != nil {
		return err
	}
	if opts.JSONOut {
		return emitJSON(opts.Stdout, resp)
	}
	RenderSAGetText(opts.Stdout, resp)
	return nil
}

// RunSADelete deletes a ServiceAccount and prints the confirmation line.
func RunSADelete(ctx context.Context, opts SADeleteOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	if err := c.SADelete(ctx, opts.SAID); err != nil {
		return err
	}
	RenderSADeleteText(opts.Stdout, opts.SAID)
	return nil
}
