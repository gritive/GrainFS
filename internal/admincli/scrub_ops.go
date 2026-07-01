package admincli

import (
	"context"
	"fmt"
	"io"
	"time"
)

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
			printFollowStopped(w, sessionID)
			return nil
		case <-t.C:
			info, err := c.GetScrubJob(ctx, sessionID)
			if err != nil {
				if ctx.Err() != nil {
					printFollowStopped(w, sessionID)
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

// printFollowStopped emits the "follow stopped, session continues" hint that
// graceful ctx-cancel paths share.
func printFollowStopped(w io.Writer, sessionID string) {
	fmt.Fprintf(w,
		"Follow stopped — session %s continues. Query `GET /v1/scrub/jobs/%s` to check status.\n",
		sessionID, sessionID)
}
