package cluster

import "fmt"

type PlacementReportOptions struct {
	Bucket  string
	Key     string
	MaxRows int
}

type PlacementReport struct {
	DesiredPolicyBasis    string                 `json:"desired_policy_basis"`
	Bucket                string                 `json:"bucket,omitempty"`
	Key                   string                 `json:"key,omitempty"`
	ObjectCount           int                    `json:"object_count"`
	Bytes                 int64                  `json:"bytes"`
	ActualProfileCounts   map[string]int         `json:"actual_profile_counts"`
	PendingUpgradeCount   int                    `json:"pending_upgrade_count"`
	DowngradeSkippedCount int                    `json:"downgrade_skipped_count"`
	UnknownLayoutCount    int                    `json:"unknown_layout_count"`
	RepairNeededCount     int                    `json:"repair_needed_count"`
	Details               []PlacementReportEntry `json:"details,omitempty"`
}

type PlacementReportEntry struct {
	Bucket           string      `json:"bucket"`
	Key              string      `json:"key"`
	VersionID        string      `json:"version_id"`
	PlacementGroupID string      `json:"placement_group_id"`
	ActualECData     uint8       `json:"actual_ec_data"`
	ActualECParity   uint8       `json:"actual_ec_parity"`
	DesiredECData    int         `json:"desired_ec_data"`
	DesiredECParity  int         `json:"desired_ec_parity"`
	LayoutState      LayoutState `json:"layout_state"`
	NodeIDs          []string    `json:"node_ids,omitempty"`
	Size             int64       `json:"size"`
}

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
				LayoutState:      state,
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
