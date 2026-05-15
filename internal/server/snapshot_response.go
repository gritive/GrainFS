package server

import "github.com/gritive/GrainFS/internal/snapshot"

type snapshotInfo struct {
	Seq         uint64 `json:"seq"`
	Timestamp   string `json:"timestamp"`
	ObjectCount int    `json:"object_count"`
	SizeBytes   int64  `json:"size_bytes"`
	Reason      string `json:"reason,omitempty"`
}

func listSnapshotsResponse(snaps []*snapshot.Snapshot) map[string]interface{} {
	items := make([]snapshotInfo, len(snaps))
	for i, sn := range snaps {
		items[i] = snapshotInfo{
			Seq:         sn.Seq,
			Timestamp:   sn.Timestamp.Format("2006-01-02T15:04:05Z"),
			ObjectCount: sn.ObjectCount,
			SizeBytes:   sn.SizeBytes,
			Reason:      sn.Reason,
		}
	}
	hint := ""
	if len(items) == 0 {
		hint = "no snapshots yet — POST /admin/snapshots to create one"
	}
	return map[string]interface{}{
		"snapshots": items,
		"hint":      hint,
	}
}
