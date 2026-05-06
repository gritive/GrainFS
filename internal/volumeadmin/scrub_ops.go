package volumeadmin

import (
	"context"
	"fmt"
	"io"
	"time"
)

// RunScrub runs `grainfs volume scrub`. When opts.Detach is false (default)
// it polls the session and prints a final summary line when status reaches
// done/cancelled.
func RunScrub(ctx context.Context, opts ScrubOptions) error {
	c, err := clientFor(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.BaseOptions)
	defer cancel()

	resp, err := c.TriggerScrub(ctx, ScrubTriggerReq{
		Name: opts.Name, Scope: opts.Scope, DryRun: opts.DryRun,
	})
	if err != nil {
		return err
	}
	if opts.JSONOut {
		return printJSON(stdout(opts.BaseOptions), resp)
	}
	created := "reused"
	if resp.Created {
		created = "created"
	}
	fmt.Fprintf(stdout(opts.BaseOptions),
		"Triggered scrub: session=%s scope=%s dry_run=%t (%s)\n",
		resp.SessionID, opts.Scope, opts.DryRun, created)
	if opts.Detach {
		return nil
	}
	return FollowScrubSession(ctx, c, stdout(opts.BaseOptions), resp.SessionID, opts.PollInterval)
}

// FollowScrubSession polls a scrub session at the given interval and writes
// terminal-state summary lines to w. interval <= 0 falls back to 1s. Exits
// gracefully on ctx cancellation with a hint about how to resume. Exposed
// so the bucket-scrub CLI can reuse the same loop.
func FollowScrubSession(ctx context.Context, c *Client, w io.Writer, sessionID string, interval time.Duration) error {
	if interval <= 0 {
		interval = time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			fmt.Fprintf(w,
				"Follow stopped — session %s continues. Run `grainfs volume scrub status %s` to check.\n",
				sessionID, sessionID)
			return nil
		case <-t.C:
			info, err := c.GetScrubJob(ctx, sessionID)
			if err != nil {
				if ctx.Err() != nil {
					fmt.Fprintf(w,
						"Follow stopped — session %s continues. Run `grainfs volume scrub status %s` to check.\n",
						sessionID, sessionID)
					return nil
				}
				return err
			}
			if info.Status == "done" || info.Status == "cancelled" {
				fmt.Fprintf(w,
					"%s. Checked=%d Healthy=%d Detected=%d Repaired=%d Unrepairable=%d\n",
					Capitalize(info.Status), info.Checked, info.Healthy, info.Detected, info.Repaired, info.Unrepairable)
				return nil
			}
		}
	}
}

// RunScrubStatus runs `grainfs volume scrub status`.
func RunScrubStatus(ctx context.Context, opts ScrubStatusOptions) error {
	c, err := clientFor(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.BaseOptions)
	defer cancel()
	info, err := c.GetScrubJob(ctx, opts.SessionID)
	if err != nil {
		return err
	}
	if opts.JSONOut {
		return printJSON(stdout(opts.BaseOptions), info)
	}
	renderScrubJobDetail(stdout(opts.BaseOptions), info)
	return nil
}

// RunScrubList runs `grainfs volume scrub list`.
func RunScrubList(ctx context.Context, opts ScrubListOptions) error {
	c, err := clientFor(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.BaseOptions)
	defer cancel()
	resp, err := c.ListScrubJobs(ctx)
	if err != nil {
		return err
	}
	if opts.JSONOut {
		return printJSON(stdout(opts.BaseOptions), resp)
	}
	renderScrubJobTable(stdout(opts.BaseOptions), resp.Jobs)
	return nil
}

// RunScrubCancel runs `grainfs volume scrub cancel`.
func RunScrubCancel(ctx context.Context, opts ScrubCancelOptions) error {
	c, err := clientFor(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.BaseOptions)
	defer cancel()
	if err := c.CancelScrub(ctx, opts.SessionID); err != nil {
		return err
	}
	if !opts.JSONOut {
		fmt.Fprintf(stdout(opts.BaseOptions), "Cancelled %s\n", opts.SessionID)
	}
	return nil
}
