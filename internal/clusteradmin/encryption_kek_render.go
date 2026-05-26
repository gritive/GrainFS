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
// Each version row shows V=, status=, seal_count=, leases=, nonce= and an
// annotation: "(current active)" for the active version, "(previous active)"
// for the immediately-preceding version (active-1) when present — the version
// a freshly retired/pruned operator most likely just rotated away from.
func RenderKEKStatus(w io.Writer, s *KEKStatus) {
	fmt.Fprintf(w, "active_version: %d\n", s.ActiveVersion)
	if len(s.Versions) == 0 {
		fmt.Fprintln(w, "versions: (none)")
		return
	}
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
		fmt.Fprintf(tw, "  V=%d\tstatus=%s\tseal_count=%d\tleases=%d\tnonce=%s%s\n",
			v.Version, v.Status, v.SealCount, v.LeaseCount, v.NonceCollisionRisk, annot)
	}
	_ = tw.Flush()
}
