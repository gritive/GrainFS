package clusteradmin

import (
	"fmt"
	"io"
	"text/tabwriter"
)

// RenderKEKStatus writes the human-readable text form of a KEK status response
// to w. The CLI (`grainfs encrypt kek status`, text format) delegates here so
// the cmd package stays a thin runner; --format json dumps the raw response
// instead.
//
// Each KEK version row shows V=, status=, leases= and an annotation:
// "(current active)" for the active version, "(previous active)" for the
// immediately-preceding version (active-1) when present — the version a freshly
// retired/pruned operator most likely just rotated away from.
//
// Seal-count / nonce-collision diagnostics are reported in a separate
// dek_generations section: AES-GCM nonce exhaustion is per-DEK-key, not per-KEK
// (a KEK rotation re-wraps the DEK without resetting the nonce count).
func RenderKEKStatus(w io.Writer, s *KEKStatus) {
	fmt.Fprintf(w, "active_version: %d\n", s.ActiveVersion)
	fmt.Fprintf(w, "active_dek_generation: %d\n", s.ActiveDEKGeneration)
	if len(s.Versions) == 0 {
		fmt.Fprintln(w, "versions: (none)")
	} else {
		fmt.Fprintln(w, "versions:")
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		for _, v := range s.Versions {
			annot := ""
			switch {
			case v.Version == s.ActiveVersion:
				annot = "  (current active)"
			case s.ActiveVersion > 0 && v.Version == s.ActiveVersion-1:
				annot = "  (previous active)"
			}
			fmt.Fprintf(tw, "  V=%d\tstatus=%s\tleases=%d%s\n",
				v.Version, v.Status, v.LeaseCount, annot)
		}
		_ = tw.Flush()
	}

	if len(s.DEKGenerations) == 0 {
		fmt.Fprintln(w, "dek_generations: (none)")
		return
	}
	fmt.Fprintln(w, "dek_generations:")
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	for _, g := range s.DEKGenerations {
		annot := ""
		if g.Active {
			annot = "  (active)"
		}
		fmt.Fprintf(tw, "  gen=%d\tseal_count=%d\tnonce=%s%s\n",
			g.Generation, g.SealCount, g.NonceCollisionRisk, annot)
	}
	_ = tw.Flush()
}
