package adminapi

import (
	"encoding/json"
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

func TestVolumeAndSnapshotJSONShape(t *testing.T) {
	payload := struct {
		Volume   VolumeInfo   `json:"volume"`
		Snapshot SnapshotInfo `json:"snapshot"`
	}{
		Volume: VolumeInfo{
			Name:            "v1",
			Size:            1024,
			BlockSize:       4096,
			AllocatedBlocks: 2,
			AllocatedBytes:  8192,
			SnapshotCount:   1,
			Health:          "ok",
			HealthReasons:   []string{},
		},
		Snapshot: SnapshotInfo{ID: "snap-1", CreatedAt: "2026-01-01T00:00:00Z", BlockCount: 2},
	}
	buf, err := json.Marshal(payload)
	require.NoError(t, err)
	var fields map[string]any
	require.NoError(t, json.Unmarshal(buf, &fields))
	require.IsType(t, map[string]any{}, fields["volume"])
	require.IsType(t, map[string]any{}, fields["snapshot"])
	volume := fields["volume"].(map[string]any)
	snapshot := fields["snapshot"].(map[string]any)
	for _, key := range []string{"block_size", "allocated_blocks", "allocated_bytes", "snapshot_count", "health_reasons"} {
		if _, ok := volume[key]; !ok {
			require.Failf(t, "missing volume JSON key", "missing volume JSON key %q in %s", key, buf)
		}
	}
	for _, key := range []string{"created_at", "block_count"} {
		if _, ok := snapshot[key]; !ok {
			require.Failf(t, "missing snapshot JSON key", "missing snapshot JSON key %q in %s", key, buf)
		}
	}
}
