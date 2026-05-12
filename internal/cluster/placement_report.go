package cluster

import (
	"fmt"

	"github.com/gritive/GrainFS/internal/adminapi"
)

type PlacementReportOptions struct {
	Bucket  string
	Key     string
	MaxRows int
}

// PlacementReport and PlacementReportEntry are aliases to the wire types in
// adminapi (single source of truth). LayoutState on the entry is a plain
// string on the wire; cluster builders convert their typed LayoutState via
// string(...) at the assignment site.
type (
	PlacementReport      = adminapi.PlacementReport
	PlacementReportEntry = adminapi.PlacementReportEntry
)

func BuildPlacementReport(entries []ObjectIndexEntry, groups map[string]ShardGroupEntry, opts PlacementReportOptions) PlacementReport {
	if opts.MaxRows <= 0 {
		opts.MaxRows = 100
	}
	report := PlacementReport{
		DesiredPolicyBasis:  "group_voter_count",
		Bucket:              opts.Bucket,
		Key:                 opts.Key,
		ActualProfileCounts: make(map[string]int),
	}
	for _, entry := range entries {
		if opts.Bucket != "" && entry.Bucket != opts.Bucket {
			continue
		}
		if opts.Key != "" && entry.Key != opts.Key {
			continue
		}

		group := groups[entry.PlacementGroupID]
		state := ClassifyObjectLayout(entry, group, RepairSignals{})
		desired := DesiredECConfigForGroup(group)
		report.ObjectCount++
		report.Bytes += entry.Size
		report.ActualProfileCounts[formatECProfile(int(entry.ECData), int(entry.ECParity))]++
		switch state {
		case LayoutPendingUpgrade:
			report.PendingUpgradeCount++
		case LayoutDowngradeSkipped:
			report.DowngradeSkippedCount++
		case LayoutUnknown:
			report.UnknownLayoutCount++
		case LayoutRepairNeeded:
			report.RepairNeededCount++
		}
		if len(report.Details) < opts.MaxRows {
			report.Details = append(report.Details, PlacementReportEntry{
				Bucket:           entry.Bucket,
				Key:              entry.Key,
				VersionID:        entry.VersionID,
				PlacementGroupID: entry.PlacementGroupID,
				ActualECData:     entry.ECData,
				ActualECParity:   entry.ECParity,
				DesiredECData:    desired.DataShards,
				DesiredECParity:  desired.ParityShards,
				LayoutState:      string(state),
				NodeIDs:          cloneStringSlice(entry.NodeIDs),
				Size:             entry.Size,
			})
		}
	}
	return report
}

func formatECProfile(k, m int) string {
	return fmt.Sprintf("%d+%d", k, m)
}
