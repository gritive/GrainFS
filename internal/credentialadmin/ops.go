package credentialadmin

import (
	"context"
	"errors"
	"io"
	"os"
	"time"
)

var NewClientFunc = NewClient

func RunCreate(ctx context.Context, opts CreateOptions) error {
	if opts.SAID == "" {
		return errors.New("--sa required")
	}
	if opts.Protocol == "" {
		return errors.New("--protocol required")
	}
	if opts.Resource == "" {
		return errors.New("--resource required")
	}
	if opts.Mode == "" {
		return errors.New("--mode required")
	}
	c, err := NewClientFunc(opts.Endpoint)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	resp, err := c.Create(ctx, CreateReq{
		SAID:      opts.SAID,
		Protocol:  opts.Protocol,
		Resource:  opts.Resource,
		Mode:      opts.Mode,
		ExpiresAt: opts.ExpiresAt,
	})
	if err != nil {
		return err
	}
	if opts.JSONOut {
		return printJSON(stdout(opts.BaseOptions), resp)
	}
	RenderCredential(stdout(opts.BaseOptions), resp)
	return nil
}

func RunList(ctx context.Context, opts ListOptions) error {
	c, err := NewClientFunc(opts.Endpoint)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	resp, err := c.ListFiltered(ctx, opts.SAID, opts.Protocol, opts.Resource)
	if err != nil {
		return err
	}
	if opts.JSONOut {
		return printJSON(stdout(opts.BaseOptions), resp)
	}
	RenderList(stdout(opts.BaseOptions), resp.Credentials)
	return nil
}

func RunGet(ctx context.Context, opts GetOptions) error {
	c, err := NewClientFunc(opts.Endpoint)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	resp, err := c.GetCredential(ctx, opts.ID)
	if err != nil {
		return err
	}
	if opts.JSONOut {
		return printJSON(stdout(opts.BaseOptions), resp)
	}
	RenderCredential(stdout(opts.BaseOptions), resp)
	return nil
}

func RunRotate(ctx context.Context, opts RotateOptions) error {
	c, err := NewClientFunc(opts.Endpoint)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	resp, err := c.Rotate(ctx, opts.ID)
	if err != nil {
		return err
	}
	if opts.JSONOut {
		return printJSON(stdout(opts.BaseOptions), resp)
	}
	RenderCredential(stdout(opts.BaseOptions), resp)
	return nil
}

func RunRevoke(ctx context.Context, opts RevokeOptions) error {
	c, err := NewClientFunc(opts.Endpoint)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	resp, err := c.Revoke(ctx, opts.ID)
	if err != nil {
		return err
	}
	if opts.JSONOut {
		return printJSON(stdout(opts.BaseOptions), resp)
	}
	if resp.Revoked {
		_, err = io.WriteString(stdout(opts.BaseOptions), "revoked "+resp.ID+"\n")
	}
	return err
}

func withTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout > 0 {
		return context.WithTimeout(ctx, timeout)
	}
	return ctx, func() {}
}

func stdout(opts BaseOptions) io.Writer {
	if opts.Stdout != nil {
		return opts.Stdout
	}
	return os.Stdout
}
