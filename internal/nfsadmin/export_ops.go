package nfsadmin

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
)

func clientFor(opts BaseOptions) (*Client, error) { return NewClient(opts.Endpoint) }

func withTimeout(ctx context.Context, opts BaseOptions) (context.Context, context.CancelFunc) {
	if opts.Timeout > 0 {
		return context.WithTimeout(ctx, opts.Timeout)
	}
	return ctx, func() {}
}

func stdout(opts BaseOptions) io.Writer {
	if opts.Stdout != nil {
		return opts.Stdout
	}
	return os.Stdout
}

func stderr(opts BaseOptions) io.Writer {
	if opts.Stderr != nil {
		return opts.Stderr
	}
	return os.Stderr
}

func RunAdd(ctx context.Context, opts AddExportOptions) error {
	if opts.Bucket == "" {
		return errors.New("bucket required")
	}
	if opts.DryRun {
		if !opts.Quiet {
			fmt.Fprintf(stdout(opts.BaseOptions), "would add NFS export %q\n", opts.Bucket)
		}
		return nil
	}
	c, err := clientFor(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.BaseOptions)
	defer cancel()
	info, err := c.UpsertExport(ctx, NfsExportUpsertReq{Bucket: opts.Bucket, ReadOnly: opts.ReadOnly})
	if err != nil {
		if !opts.JSONOut {
			RenderError(stderr(opts.BaseOptions), err)
		}
		return err
	}
	return renderMutation(opts.BaseOptions, opts.Quiet, "added", info)
}

func RunUpdate(ctx context.Context, opts UpdateExportOptions) error {
	if opts.Bucket == "" {
		return errors.New("bucket required")
	}
	if opts.DryRun {
		if !opts.Quiet {
			fmt.Fprintf(stdout(opts.BaseOptions), "would update NFS export %q\n", opts.Bucket)
		}
		return nil
	}
	c, err := clientFor(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.BaseOptions)
	defer cancel()
	info, err := c.UpdateExport(ctx, opts.Bucket, NfsExportUpsertReq{ReadOnly: opts.ReadOnly})
	if err != nil {
		if !opts.JSONOut {
			RenderError(stderr(opts.BaseOptions), err)
		}
		return err
	}
	return renderMutation(opts.BaseOptions, opts.Quiet, "updated", info)
}

func RunRemove(ctx context.Context, opts RemoveExportOptions) error {
	if opts.Bucket == "" {
		return errors.New("bucket required")
	}
	if opts.DryRun {
		if !opts.Quiet {
			fmt.Fprintf(stdout(opts.BaseOptions), "would remove NFS export %q\n", opts.Bucket)
		}
		return nil
	}
	c, err := clientFor(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.BaseOptions)
	defer cancel()
	if err := c.DeleteExport(ctx, opts.Bucket); err != nil {
		return err
	}
	if !opts.Quiet && !opts.JSONOut {
		fmt.Fprintf(stdout(opts.BaseOptions), "removed NFS export %q\n", opts.Bucket)
	}
	return nil
}

func RunList(ctx context.Context, opts ListExportOptions) error {
	c, err := clientFor(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.BaseOptions)
	defer cancel()
	resp, err := c.ListExports(ctx)
	if err != nil {
		return err
	}
	return RenderExportList(stdout(opts.BaseOptions), opts.JSONOut, resp.Exports)
}

func renderMutation(opts BaseOptions, quiet bool, verb string, info NfsExportInfo) error {
	if opts.JSONOut {
		return RenderExportList(stdout(opts), true, []NfsExportInfo{info})
	}
	if quiet {
		return nil
	}
	mode := "rw"
	if info.ReadOnly {
		mode = "ro"
	}
	_, err := fmt.Fprintf(stdout(opts), "%s NFS export %q (%s, fsid=%d.%d, gen=%d)\n",
		verb, info.Bucket, mode, info.FsidMajor, info.FsidMinor, info.Generation)
	return err
}
