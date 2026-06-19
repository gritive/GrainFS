package clusteradmin

import (
	"context"
	"fmt"
	"io"
	"net/url"

	"github.com/gritive/GrainFS/internal/server"
)

// VerifyPerVersionCutoverOptions configures RunVerifyPerVersion.
type VerifyPerVersionCutoverOptions struct {
	Endpoint string
	Bucket   string
	Out      io.Writer
}

// VerifyPerVersionCutover issues GET /v1/cluster/verify-per-version-cutover
// with an optional bucket filter.
func (c *Client) VerifyPerVersionCutover(ctx context.Context, bucket string) (*server.PerVersionCutoverReadiness, error) {
	path := "/v1/cluster/verify-per-version-cutover"
	if bucket != "" {
		path += "?" + url.Values{"bucket": {bucket}}.Encode()
	}
	var out server.PerVersionCutoverReadiness
	if err := c.Get(ctx, path, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// RunVerifyPerVersion is the thin-runner entry point for
// `grainfs cluster verify-per-version`.
// It prints the 5 readiness counts, the capped gap/stuck/unknown ref lists,
// and a node-local scope note. Returns a non-nil error (→ non-zero exit) if
// gaps+stuck+unknown > 0.
func RunVerifyPerVersion(ctx context.Context, opts VerifyPerVersionCutoverOptions) error {
	r, err := NewClient(opts.Endpoint).VerifyPerVersionCutover(ctx, opts.Bucket)
	if err != nil {
		return err
	}

	w := opts.Out
	fmt.Fprintln(w, "per-version cutover readiness (this node only):")
	fmt.Fprintf(w, "  complete: %d\n", r.Complete)
	fmt.Fprintf(w, "  gaps:     %d\n", r.Gaps)
	fmt.Fprintf(w, "  stuck:    %d\n", r.Stuck)
	fmt.Fprintf(w, "  unknown:  %d\n", r.Unknown)
	fmt.Fprintf(w, "  excluded: %d\n", r.Excluded)
	fmt.Fprintf(w, "  ineligible: %d\n", r.Ineligible)

	if len(r.GapRefs) > 0 {
		fmt.Fprintln(w, "gap refs (backfillable):")
		for _, ref := range r.GapRefs {
			fmt.Fprintf(w, "  %s\n", ref)
		}
	}
	if len(r.StuckRefs) > 0 {
		fmt.Fprintln(w, "stuck refs (no placement — manual attention required):")
		for _, ref := range r.StuckRefs {
			fmt.Fprintf(w, "  %s\n", ref)
		}
	}
	if len(r.UnknownRefs) > 0 {
		fmt.Fprintln(w, "unknown refs (decode/RPC error — investigate):")
		for _, ref := range r.UnknownRefs {
			fmt.Fprintf(w, "  %s\n", ref)
		}
	}

	fmt.Fprintln(w, "note: this node only — cluster-readiness requires every node to report 0 gaps+stuck+unknown+ineligible")

	// Ineligible > 0 blocks readiness: a cutover-ineligible (non-Enabled) bucket
	// is a deferred epic and must never read as ready, regardless of object counts.
	// This is the per-bucket flip-eligibility gate (S4c-d independently refuses
	// non-Enabled buckets).
	if r.Gaps+r.Stuck+r.Unknown+r.Ineligible > 0 {
		return fmt.Errorf("cutover NOT ready: gaps=%d stuck=%d unknown=%d ineligible=%d", r.Gaps, r.Stuck, r.Unknown, r.Ineligible)
	}
	return nil
}
