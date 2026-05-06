package volumeadmin

import (
	"context"
	"fmt"
)

// RunSnapshotCreate runs `grainfs volume snapshot create`.
func RunSnapshotCreate(ctx context.Context, opts SnapshotCreateOptions) error {
	c, err := clientFor(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.BaseOptions)
	defer cancel()
	resp, err := c.CreateSnapshot(ctx, opts.Volume)
	if err != nil {
		return err
	}
	if opts.JSONOut {
		return printJSON(stdout(opts.BaseOptions), resp)
	}
	fmt.Fprintf(stdout(opts.BaseOptions), "snapshot %q created (blocks: %d)\n", resp.ID, resp.BlockCount)
	return nil
}

// RunSnapshotList runs `grainfs volume snapshot list`.
func RunSnapshotList(ctx context.Context, opts SnapshotListOptions) error {
	c, err := clientFor(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.BaseOptions)
	defer cancel()
	snaps, err := c.ListSnapshots(ctx, opts.Volume)
	if err != nil {
		return err
	}
	if opts.JSONOut {
		return printJSON(stdout(opts.BaseOptions), snaps)
	}
	renderSnapshotTable(stdout(opts.BaseOptions), snaps)
	return nil
}

// RunSnapshotDelete runs `grainfs volume snapshot delete`.
func RunSnapshotDelete(ctx context.Context, opts SnapshotDeleteOptions) error {
	c, err := clientFor(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.BaseOptions)
	defer cancel()
	if err := c.DeleteSnapshot(ctx, opts.Volume, opts.SnapshotID); err != nil {
		return err
	}
	if !opts.JSONOut {
		fmt.Fprintf(stdout(opts.BaseOptions), "snapshot %q deleted from %q\n", opts.SnapshotID, opts.Volume)
	}
	return nil
}
