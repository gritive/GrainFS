package volumeadmin

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
)

// clientFor returns a Client honoring opts.Endpoint via ResolveEndpoint
// (flag → env).
func clientFor(opts BaseOptions) (*Client, error) {
	return NewClient(opts.Endpoint)
}

// withTimeout wraps ctx with opts.Timeout if positive.
func withTimeout(ctx context.Context, opts BaseOptions) (context.Context, context.CancelFunc) {
	if opts.Timeout > 0 {
		return context.WithTimeout(ctx, opts.Timeout)
	}
	return ctx, func() {}
}

// stdout returns opts.Stdout or os.Stdout when nil.
func stdout(opts BaseOptions) io.Writer {
	if opts.Stdout != nil {
		return opts.Stdout
	}
	return os.Stdout
}

// stderr returns opts.Stderr or os.Stderr when nil.
func stderr(opts BaseOptions) io.Writer {
	if opts.Stderr != nil {
		return opts.Stderr
	}
	return os.Stderr
}

// RunList runs `grainfs volume list`.
func RunList(ctx context.Context, opts ListOptions) error {
	c, err := clientFor(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.BaseOptions)
	defer cancel()
	resp, err := c.ListVolumes(ctx)
	if err != nil {
		return err
	}
	if opts.JSONOut {
		return printJSON(stdout(opts.BaseOptions), resp)
	}
	renderVolumeTable(stdout(opts.BaseOptions), resp.Volumes, opts.RawBytes)
	return nil
}

// RunCreate runs `grainfs volume create`.
func RunCreate(ctx context.Context, opts CreateOptions) error {
	if opts.Name == "" {
		return errors.New("name required")
	}
	if opts.Size <= 0 {
		return errors.New("size must be positive")
	}
	c, err := clientFor(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.BaseOptions)
	defer cancel()
	resp, err := c.CreateVolume(ctx, CreateVolumeReq{Name: opts.Name, Size: opts.Size})
	if err != nil {
		return err
	}
	if opts.JSONOut {
		return printJSON(stdout(opts.BaseOptions), resp)
	}
	fmt.Fprintf(stdout(opts.BaseOptions), "created %q (size=%s, block=%d)\n",
		resp.Name, FormatBytes(resp.Size, false), resp.BlockSize)
	return nil
}

// RunInfo runs `grainfs volume info`.
func RunInfo(ctx context.Context, opts InfoOptions) error {
	c, err := clientFor(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.BaseOptions)
	defer cancel()
	resp, err := c.GetVolume(ctx, opts.Name)
	if err != nil {
		return err
	}
	if opts.JSONOut {
		return printJSON(stdout(opts.BaseOptions), resp)
	}
	renderVolumeInfo(stdout(opts.BaseOptions), resp, opts.RawBytes)
	return nil
}

// RunStat runs `grainfs volume stat`.
func RunStat(ctx context.Context, opts StatOptions) error {
	c, err := clientFor(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.BaseOptions)
	defer cancel()
	resp, err := c.StatVolume(ctx, opts.Name)
	if err != nil {
		return err
	}
	if opts.JSONOut {
		return printJSON(stdout(opts.BaseOptions), resp)
	}
	renderVolumeStat(stdout(opts.BaseOptions), resp, opts.RawBytes)
	return nil
}

// RunDelete runs `grainfs volume delete`. On 409 conflict (no --force,
// snapshots present) it writes the typed conflict block to Stderr before
// returning the error so cmd can exit non-zero.
func RunDelete(ctx context.Context, opts DeleteOptions) error {
	c, err := clientFor(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.BaseOptions)
	defer cancel()
	resp, err := c.DeleteVolume(ctx, opts.Name, opts.Force)
	var e *Error
	if errors.As(err, &e) && e.Code == "conflict" && !opts.JSONOut {
		FormatDeleteConflict(stderr(opts.BaseOptions), e)
		return err
	}
	if err != nil {
		return err
	}
	if opts.JSONOut {
		return printJSON(stdout(opts.BaseOptions), resp)
	}
	fmt.Fprintf(stdout(opts.BaseOptions), "deleted %q\n", opts.Name)
	return nil
}

// RunResize runs `grainfs volume resize`. On unsupported (shrink) it writes
// the hint block to Stderr.
func RunResize(ctx context.Context, opts ResizeOptions) error {
	if opts.Size <= 0 {
		return errors.New("size must be positive")
	}
	c, err := clientFor(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.BaseOptions)
	defer cancel()
	resp, err := c.ResizeVolume(ctx, opts.Name, opts.Size)
	var e *Error
	if errors.As(err, &e) && e.Code == "unsupported" && !opts.JSONOut {
		FormatResizeUnsupported(stderr(opts.BaseOptions), e)
		return err
	}
	if err != nil {
		return err
	}
	if opts.JSONOut {
		return printJSON(stdout(opts.BaseOptions), resp)
	}
	if !resp.Changed {
		fmt.Fprintf(stdout(opts.BaseOptions), "no change (size already %s)\n", FormatBytes(resp.NewSize, false))
		return nil
	}
	fmt.Fprintf(stdout(opts.BaseOptions), "resized %q: %s → %s\n",
		resp.Name, FormatBytes(resp.OldSize, false), FormatBytes(resp.NewSize, false))
	return nil
}

// RunRecalculate runs `grainfs volume recalculate`.
func RunRecalculate(ctx context.Context, opts RecalculateOptions) error {
	c, err := clientFor(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.BaseOptions)
	defer cancel()
	resp, err := c.RecalculateVolume(ctx, opts.Name)
	if err != nil {
		return err
	}
	if opts.JSONOut {
		return printJSON(stdout(opts.BaseOptions), resp)
	}
	status := "no change"
	if resp.Fixed {
		status = "fixed"
	}
	fmt.Fprintf(stdout(opts.BaseOptions), "recalculated %q: %d → %d (%s)\n",
		resp.Volume, resp.Before, resp.After, status)
	return nil
}

// RunClone runs `grainfs volume clone`.
func RunClone(ctx context.Context, opts CloneOptions) error {
	c, err := clientFor(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.BaseOptions)
	defer cancel()
	if err := c.CloneVolume(ctx, opts.Src, opts.Dst); err != nil {
		return err
	}
	if !opts.JSONOut {
		fmt.Fprintf(stdout(opts.BaseOptions), "cloned %q → %q\n", opts.Src, opts.Dst)
	}
	return nil
}

// RunRollback runs `grainfs volume rollback`.
func RunRollback(ctx context.Context, opts RollbackOptions) error {
	c, err := clientFor(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.BaseOptions)
	defer cancel()
	if err := c.RollbackVolume(ctx, opts.Name, opts.SnapshotID); err != nil {
		return err
	}
	if !opts.JSONOut {
		fmt.Fprintf(stdout(opts.BaseOptions), "rolled back %q to snapshot %q\n", opts.Name, opts.SnapshotID)
	}
	return nil
}

// RunWriteAt runs `grainfs volume write-at`.
func RunWriteAt(ctx context.Context, opts WriteAtOptions) error {
	c, err := clientFor(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.BaseOptions)
	defer cancel()
	resp, err := c.WriteAtVolume(ctx, opts.Name, opts.Offset, opts.Content)
	if err != nil {
		return err
	}
	if opts.JSONOut {
		return printJSON(stdout(opts.BaseOptions), resp)
	}
	fmt.Fprintf(stdout(opts.BaseOptions), "wrote %d bytes to %q at offset %d\n", resp.Bytes, opts.Name, opts.Offset)
	return nil
}

// RunReadAt runs `grainfs volume read-at`. In non-JSON mode the bytes are
// written verbatim to stdout (the caller is expected to redirect).
func RunReadAt(ctx context.Context, opts ReadAtOptions) error {
	c, err := clientFor(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.BaseOptions)
	defer cancel()
	resp, err := c.ReadAtVolume(ctx, opts.Name, opts.Offset, opts.Length)
	if err != nil {
		return err
	}
	if opts.JSONOut {
		return printJSON(stdout(opts.BaseOptions), resp)
	}
	_, err = stdout(opts.BaseOptions).Write(resp.Data)
	return err
}
