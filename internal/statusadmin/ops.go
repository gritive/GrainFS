package statusadmin

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
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
	fmt.Fprintf(tw, "\n")
	fmt.Fprintf(tw, "sa_count:\t%d\n", report.IAM.SACount)
	fmt.Fprintf(tw, "banner:\t%v\n", report.Banner)
	fmt.Fprintf(tw, "\n")
	fmt.Fprintf(tw, "encryption_enabled:\t%v\n", report.Encryption.Enabled)
	fmt.Fprintf(tw, "dek_gen:\t%s\n", strconv.FormatUint(uint64(report.Encryption.DEKGen), 10))
	fmt.Fprintf(tw, "\n")
	fmt.Fprintf(tw, "tls_cert_present:\t%v\n", report.TLS.CertPresent)
	if len(report.TrustedProxy) > 0 {
		fmt.Fprintf(tw, "trusted_proxy:\t%s\n", strings.Join(report.TrustedProxy, ","))
	}
	fmt.Fprintf(tw, "\n")
	fmt.Fprintf(tw, "audit_deny_only:\t%v\n", report.Audit.DenyOnly)
	fmt.Fprintf(tw, "\n")
	if report.JWTKeys.CurrentKID != "" {
		fmt.Fprintf(tw, "jwt_current_kid:\t%s\n", report.JWTKeys.CurrentKID)
	}
	if report.JWTKeys.PreviousKID != "" {
		fmt.Fprintf(tw, "jwt_previous_kid:\t%s\n", report.JWTKeys.PreviousKID)
	}
	return tw.Flush()
}
