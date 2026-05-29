package iamadmin

import (
	"context"
	"fmt"
	"os"
	"strings"
)

// RunPDPSetToken reads the bearer token from opts.TokenFile (trailing newline
// trimmed), then seals + stores it via the admin server. The token is never
// passed on the command line — only via a file — and travels in the request
// body.
func RunPDPSetToken(ctx context.Context, opts PDPSetTokenOptions) error {
	b, err := os.ReadFile(opts.TokenFile)
	if err != nil {
		return fmt.Errorf("read token file: %w", err)
	}
	token := strings.TrimRight(string(b), "\r\n")
	if token == "" {
		return fmt.Errorf("token file %q is empty", opts.TokenFile)
	}
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	return c.PDPSetToken(ctx, token)
}

// RunPDPClearToken removes the configured External PDP bearer token.
func RunPDPClearToken(ctx context.Context, opts PDPClearTokenOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	return c.PDPClearToken(ctx)
}

// RunPDPShow prints the server-rendered PDP status (never the token).
func RunPDPShow(ctx context.Context, opts PDPShowOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	resp, err := c.PDPShow(ctx)
	if err != nil {
		return err
	}
	if opts.JSONOut {
		return emitJSON(opts.Stdout, resp)
	}
	fmt.Fprint(opts.Stdout, resp.Status)
	return nil
}
