package statusadmin

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/gritive/GrainFS/internal/adminapi"
)

func withTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if d <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, d)
}

// RunGetStatus fetches and renders the cluster status report.
func RunGetStatus(ctx context.Context, opts GetOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	report, err := c.GetStatus(ctx)
	if err != nil {
		return err
	}
	return renderStatus(opts.Stdout, report, opts.JSONOut)
}

func renderStatus(w io.Writer, report adminapi.StatusReport, asJSON bool) error {
	if asJSON {
		b, err := json.Marshal(report)
		if err != nil {
			return fmt.Errorf("marshal: %w", err)
		}
		_, err = fmt.Fprintln(w, string(b))
		return err
	}
	// Human-readable table output.
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "node_id:\t%s\n", report.Cluster.NodeID)
	fmt.Fprintf(tw, "cluster_size:\t%d\n", report.Cluster.ClusterSize)
	fmt.Fprintf(tw, "phase:\t%d\n", report.Cluster.Phase)
	fmt.Fprintf(tw, "\n")
	fmt.Fprintf(tw, "sa_count:\t%d\n", report.IAM.SACount)
	fmt.Fprintf(tw, "banner:\t%v\n", report.IAM.Banner)
	fmt.Fprintf(tw, "\n")
	fmt.Fprintf(tw, "encryption_enabled:\t%v\n", report.Encryption.Enabled)
	fmt.Fprintf(tw, "dek_gen:\t%s\n", strconv.FormatUint(uint64(report.Encryption.DEKGen), 10))
	fmt.Fprintf(tw, "\n")
	fmt.Fprintf(tw, "tls_cert_present:\t%v\n", report.TLS.CertPresent)
	if report.Proxy.TrustedCIDR != "" {
		fmt.Fprintf(tw, "trusted_cidr:\t%s\n", report.Proxy.TrustedCIDR)
	}
	fmt.Fprintf(tw, "\n")
	fmt.Fprintf(tw, "audit_deny_only:\t%v\n", report.Audit.DenyOnly)
	fmt.Fprintf(tw, "\n")
	if report.JWT.CurrentKID != "" {
		fmt.Fprintf(tw, "jwt_current_kid:\t%s\n", report.JWT.CurrentKID)
	}
	if report.JWT.PreviousKID != "" {
		fmt.Fprintf(tw, "jwt_previous_kid:\t%s\n", report.JWT.PreviousKID)
	}
	return tw.Flush()
}
