package adminapi

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestScrubJobInfoJSONRoundTrip(t *testing.T) {
	in := ScrubJobInfo{
		SessionID:    "sess-1",
		Bucket:       "bucket",
		KeyPrefix:    "prefix/",
		Scope:        "full",
		DryRun:       true,
		Status:       "running",
		StartedAt:    1777777777,
		Checked:      10,
		Healthy:      8,
		Detected:     2,
		Repaired:     1,
		Unrepairable: 1,
		Skipped:      3,
		OwnedHere:    true,
		Partial:      true,
		PeerFailures: []string{"node-b"},
	}
	buf, err := json.Marshal(in)
	require.NoError(t, err)
	var fields map[string]any
	require.NoError(t, json.Unmarshal(buf, &fields))
	for _, key := range []string{"session_id", "key_prefix", "dry_run", "owned_here", "peer_failures"} {
		if _, ok := fields[key]; !ok {
			require.Failf(t, "missing JSON key", "missing JSON key %q in %s", key, buf)
		}
	}
	var out ScrubJobInfo
	require.NoError(t, json.Unmarshal(buf, &out))
	require.Equal(t, in.SessionID, out.SessionID)
	require.Equal(t, in.OwnedHere, out.OwnedHere)
	require.Len(t, out.PeerFailures, 1)
}

func TestVolumeJSONShape(t *testing.T) {
	payload := struct {
		Volume VolumeInfo `json:"volume"`
	}{
		Volume: VolumeInfo{
			Name:            "v1",
			Size:            1024,
			BlockSize:       4096,
			AllocatedBlocks: 2,
			AllocatedBytes:  8192,
			Health:          "ok",
			HealthReasons:   []string{},
		},
	}
	buf, err := json.Marshal(payload)
	require.NoError(t, err)
	var fields map[string]any
	require.NoError(t, json.Unmarshal(buf, &fields))
	require.IsType(t, map[string]any{}, fields["volume"])
	volume := fields["volume"].(map[string]any)
	for _, key := range []string{"block_size", "allocated_blocks", "allocated_bytes", "health_reasons"} {
		if _, ok := volume[key]; !ok {
			require.Failf(t, "missing volume JSON key", "missing volume JSON key %q in %s", key, buf)
		}
	}
}

func TestStatus_JSONRoundTrip(t *testing.T) {
	in := Status{
		Mode: "cluster", NodeID: "n1", State: "leader", Term: 7, LeaderID: "n1",
		Peers:             []string{"n1", "n2"},
		DownNodes:         []string{"n2"},
		PeerAddrs:         map[string]string{"n1": "127.0.0.1:7001"},
		PeerStates:        map[string]string{"n1": "live"},
		BucketAssignments: map[string]string{"b": "g1"},
		ShardGroups:       []ShardGroup{{ID: "g1", PeerIDs: []string{"n1", "n2"}, LeaderID: "n2"}},
	}
	buf, err := json.Marshal(in)
	require.NoError(t, err)
	var out Status
	require.NoError(t, json.Unmarshal(buf, &out))
	require.True(t, reflect.DeepEqual(in, out), "round-trip mismatch:\n%+v\n%+v", in, out)
}

func TestHealth_JSONRoundTrip(t *testing.T) {
	in := Health{
		Mode: "cluster", Degraded: false, LeaderID: "n1", Term: 7,
		Quorum: QuorumInfo{VotersTotal: 3, AliveCount: 3, Required: 2, Healthy: true},
		Peers: []PeerHealthRow{
			{PeerID: "n1", State: "self"},
			{PeerID: "n2", State: "live", RaftAddr: "127.0.0.1:7002"},
		},
		Issues: []string{"none"},
	}
	buf, err := json.Marshal(in)
	require.NoError(t, err)
	var out Health
	require.NoError(t, json.Unmarshal(buf, &out))
	require.True(t, reflect.DeepEqual(in, out), "round-trip mismatch:\n%+v\n%+v", in, out)
}

func TestPlacementReport_JSONRoundTrip(t *testing.T) {
	in := PlacementReport{
		DesiredPolicyBasis:  "group_voter_count",
		Bucket:              "b",
		Key:                 "k",
		ObjectCount:         2,
		Bytes:               4096,
		ActualProfileCounts: map[string]int{"4+2": 2},
		Details: []PlacementReportEntry{{
			Bucket: "b", Key: "k", VersionID: "v", PlacementGroupID: "g1",
			ActualECData: 4, ActualECParity: 2, DesiredECData: 4, DesiredECParity: 2,
			LayoutState: "ok", NodeIDs: []string{"n1", "n2"}, Size: 2048,
		}},
	}
	buf, err := json.Marshal(in)
	require.NoError(t, err)
	var out PlacementReport
	require.NoError(t, json.Unmarshal(buf, &out))
	require.True(t, reflect.DeepEqual(in, out), "round-trip mismatch:\n%+v\n%+v", in, out)
}

func TestBalancerStatus_JSONRoundTrip(t *testing.T) {
	in := BalancerStatus{
		Available: true, Active: false, ImbalancePct: 12.5,
		Nodes: []BalancerNodeStatus{{
			NodeID: "n1", DiskUsedPct: 33.3, DiskAvailBytes: 1 << 30,
			RequestsPerSec: 12.0, JoinedAt: "2026-05-12T00:00:00Z", UpdatedAt: "2026-05-12T00:01:00Z",
		}},
	}
	buf, err := json.Marshal(in)
	require.NoError(t, err)
	var out BalancerStatus
	require.NoError(t, json.Unmarshal(buf, &out))
	require.True(t, reflect.DeepEqual(in, out), "round-trip mismatch:\n%+v\n%+v", in, out)
}

func TestEvent_JSONRoundTrip(t *testing.T) {
	in := Event{
		Timestamp: 1747008000, Type: "audit", Action: "put_object",
		Bucket: "b", Key: "k", Size: 1024,
	}
	b1, err := json.Marshal(in)
	require.NoError(t, err)
	var out Event
	require.NoError(t, json.Unmarshal(b1, &out))
	b2, err := json.Marshal(out)
	require.NoError(t, err)
	require.Equal(t, string(b1), string(b2))
}
