package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildPlacementReport_AggregatesSummaryAndLimitsDetail(t *testing.T) {
	groups := map[string]ShardGroupEntry{
		"group-1": {ID: "group-1", PeerIDs: []string{"n1", "n2", "n3", "n4", "n5", "n6"}},
	}
	entries := []ObjectIndexEntry{
		{Bucket: "b", Key: "a", VersionID: "v1", PlacementGroupID: "group-1", ECData: 2, ECParity: 1, Size: 10, NodeIDs: []string{"n1", "n2", "n3"}},
		{Bucket: "b", Key: "z", VersionID: "v1", PlacementGroupID: "group-1", ECData: 4, ECParity: 2, Size: 20, NodeIDs: []string{"n1", "n2", "n3", "n4", "n5", "n6"}},
	}

	report := BuildPlacementReport(entries, groups, PlacementReportOptions{Bucket: "b", MaxRows: 1})

	require.Equal(t, "group_voter_count", report.DesiredPolicyBasis)
	require.Equal(t, 2, report.ObjectCount)
	require.Equal(t, int64(30), report.Bytes)
	require.Equal(t, 1, report.PendingUpgradeCount)
	require.Len(t, report.Details, 1)
	require.Equal(t, "a", report.Details[0].Key)
}

func TestBuildPlacementReport_RepairNeededMetadataOnly(t *testing.T) {
	groups := map[string]ShardGroupEntry{
		"group-1": {ID: "group-1", PeerIDs: []string{"n1", "n2", "n3"}},
	}
	report := BuildPlacementReport([]ObjectIndexEntry{
		{Bucket: "b", Key: "broken", VersionID: "v1", PlacementGroupID: "group-1", ECData: 2, ECParity: 1},
	}, groups, PlacementReportOptions{Bucket: "b", MaxRows: 10})

	require.Equal(t, 1, report.RepairNeededCount)
	require.Equal(t, LayoutRepairNeeded, report.Details[0].LayoutState)
}
