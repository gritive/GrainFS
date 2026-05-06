package volumeadmin

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// PrintJSON encodes v as indented JSON to w. Exposed for non-volume admin
// CLI commands (dashboard, bucket scrub) so they share one canonical
// JSON-output style.
func PrintJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// printJSON is the unexported alias kept so call sites inside this package
// stay compact.
func printJSON(w io.Writer, v any) error { return PrintJSON(w, v) }

// renderVolumeTable writes the `volume list` table to w.
func renderVolumeTable(w io.Writer, vols []VolumeInfo, raw bool) {
	fmt.Fprintf(w, "%-20s  %12s  %12s  %9s  %s\n", "NAME", "SIZE", "ALLOCATED", "SNAPSHOTS", "HEALTH")
	if len(vols) == 0 {
		fmt.Fprintln(w, "(no volumes)")
		return
	}
	for _, v := range vols {
		fmt.Fprintf(w, "%-20s  %12s  %12s  %9d  %s\n",
			v.Name, FormatBytes(v.Size, raw), FormatBytes(v.AllocatedBytes, raw),
			v.SnapshotCount, FormatVolumeHealth(v.Health))
	}
}

// renderVolumeInfo writes the `volume info` key:value block to w.
func renderVolumeInfo(w io.Writer, v VolumeInfo, raw bool) {
	fmt.Fprintf(w, "name:             %s\n", v.Name)
	fmt.Fprintf(w, "size:             %s\n", FormatBytes(v.Size, raw))
	fmt.Fprintf(w, "block_size:       %s\n", FormatBytes(int64(v.BlockSize), raw))
	fmt.Fprintf(w, "allocated_bytes:  %s\n", FormatBytes(v.AllocatedBytes, raw))
	fmt.Fprintf(w, "allocated_blocks: %d\n", v.AllocatedBlocks)
	fmt.Fprintf(w, "snapshot_count:   %d\n", v.SnapshotCount)
	fmt.Fprintf(w, "health:           %s\n", FormatVolumeHealth(v.Health))
	if len(v.HealthReasons) > 0 {
		fmt.Fprintf(w, "health_reasons:   %s\n", strings.Join(v.HealthReasons, ","))
	}
}

// renderVolumeStat writes the `volume stat` block.
func renderVolumeStat(w io.Writer, s VolumeStatResp, raw bool) {
	fmt.Fprintf(w, "volume:           %s\n", s.Volume.Name)
	fmt.Fprintf(w, "size:             %s\n", FormatBytes(s.Volume.Size, raw))
	fmt.Fprintf(w, "allocated:        %s\n", FormatBytes(s.Volume.AllocatedBytes, raw))
	fmt.Fprintf(w, "snapshots:        %d\n", s.Volume.SnapshotCount)
	fmt.Fprintf(w, "health:           %s\n", FormatVolumeHealth(s.Volume.Health))
	if len(s.Volume.HealthReasons) > 0 {
		fmt.Fprintf(w, "health_reasons:   %s\n", strings.Join(s.Volume.HealthReasons, ","))
	}
	if len(s.RecentIncidents) > 0 {
		fmt.Fprintf(w, "recent incidents: %d\n", len(s.RecentIncidents))
	}
}

// renderSnapshotTable writes the `volume snapshot list` table.
func renderSnapshotTable(w io.Writer, snaps []SnapshotInfo) {
	if len(snaps) == 0 {
		fmt.Fprintln(w, "no snapshots")
		return
	}
	fmt.Fprintf(w, "%-40s  %-30s  %s\n", "ID", "CREATED AT", "BLOCKS")
	fmt.Fprintln(w, strings.Repeat("-", 80))
	for _, s := range snaps {
		fmt.Fprintf(w, "%-40s  %-30s  %d\n", s.ID, s.CreatedAt, s.BlockCount)
	}
}

// renderScrubJobTable writes the `volume scrub list` table.
func renderScrubJobTable(w io.Writer, jobs []ScrubJobInfo) {
	if len(jobs) == 0 {
		fmt.Fprintln(w, "(no scrub sessions)")
		return
	}
	fmt.Fprintf(w, "%-38s  %-10s  %-6s  %-9s  %-9s  %-9s\n",
		"SESSION", "STATUS", "SCOPE", "CHECKED", "DETECTED", "REPAIRED")
	for _, j := range jobs {
		fmt.Fprintf(w, "%-38s  %-10s  %-6s  %9d  %9d  %9d\n",
			j.SessionID, j.Status, j.Scope, j.Checked, j.Detected, j.Repaired)
	}
}

// renderScrubJobDetail writes the `volume scrub status` block including the
// partial-peer-failure information that the previous CLI implementation
// silently dropped.
func renderScrubJobDetail(w io.Writer, j ScrubJobInfo) {
	fmt.Fprintf(w, "Session: %s\nStatus:  %s\nScope:   %s\nDryRun:  %t\nBucket:  %s\nPrefix:  %s\nChecked: %d  Healthy: %d  Detected: %d  Repaired: %d  Unrepairable: %d\n",
		j.SessionID, j.Status, j.Scope, j.DryRun,
		j.Bucket, j.KeyPrefix,
		j.Checked, j.Healthy, j.Detected, j.Repaired, j.Unrepairable)
	if j.Partial {
		if len(j.PeerFailures) > 0 {
			fmt.Fprintf(w, "Partial: yes (peer failures: %s)\n", strings.Join(j.PeerFailures, ","))
		} else {
			fmt.Fprintln(w, "Partial: yes")
		}
	}
}
