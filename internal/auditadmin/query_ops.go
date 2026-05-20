package auditadmin

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"text/tabwriter"
	"time"
)

func withTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if d <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, d)
}

// RunAuditQuery executes a raw SQL query and writes the result to opts.Stdout.
func RunAuditQuery(ctx context.Context, opts QueryOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	resp, err := c.QueryAudit(ctx, QueryRequest{SQL: opts.SQL, Limit: opts.Limit})
	if err != nil {
		return err
	}
	return renderQueryResponse(opts.Stdout, resp, opts.JSONOut)
}

// RunAuditRecentDenies fetches recent deny-outcome rows and writes the result.
func RunAuditRecentDenies(ctx context.Context, opts RecentDeniesOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	resp, err := c.RecentDenies(ctx, opts.Limit)
	if err != nil {
		return err
	}
	return renderQueryResponse(opts.Stdout, resp, opts.JSONOut)
}

// RunAuditBySA fetches audit rows for a given service account.
func RunAuditBySA(ctx context.Context, opts BySAOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	resp, err := c.BySA(ctx, opts.SAID, opts.Limit)
	if err != nil {
		return err
	}
	return renderQueryResponse(opts.Stdout, resp, opts.JSONOut)
}

// RunAuditByRequestID fetches audit rows for a given request ID.
func RunAuditByRequestID(ctx context.Context, opts ByRequestIDOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	resp, err := c.ByRequestID(ctx, opts.RequestID)
	if err != nil {
		return err
	}
	return renderQueryResponse(opts.Stdout, resp, opts.JSONOut)
}

func renderQueryResponse(w io.Writer, resp QueryResponse, asJSON bool) error {
	if asJSON {
		b, err := json.Marshal(resp)
		if err != nil {
			return fmt.Errorf("marshal: %w", err)
		}
		_, err = fmt.Fprintln(w, string(b))
		return err
	}
	if len(resp.Rows) == 0 {
		fmt.Fprintln(w, "(no rows)")
		return nil
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	for i, col := range resp.Columns {
		if i > 0 {
			fmt.Fprint(tw, "\t")
		}
		fmt.Fprint(tw, col)
	}
	fmt.Fprintln(tw)
	for _, row := range resp.Rows {
		for i, cell := range row {
			if i > 0 {
				fmt.Fprint(tw, "\t")
			}
			fmt.Fprint(tw, cell)
		}
		fmt.Fprintln(tw)
	}
	return tw.Flush()
}
