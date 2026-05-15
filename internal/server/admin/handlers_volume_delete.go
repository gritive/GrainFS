package admin

import (
	"fmt"

	"github.com/gritive/GrainFS/internal/volume"
)

func volumeDeleteSnapshotConflict(name string, snaps []volume.SnapshotInfo) error {
	recent := snaps
	if len(recent) > 3 {
		recent = recent[:3]
	}
	recentInfos := make([]SnapshotInfo, len(recent))
	for i, snap := range recent {
		recentInfos[i] = snapshotToInfo(snap)
	}
	return NewConflict(
		fmt.Sprintf("volume %q has %d snapshots; refused without --force", name, len(snaps)),
		map[string]any{
			"snapshot_count":  len(snaps),
			"recent":          recentInfos,
			"cascade_command": fmt.Sprintf("grainfs volume delete %s --force", name),
			"list_command":    fmt.Sprintf("grainfs volume snapshot list %s", name),
		},
	)
}

func deleteVolumeData(d *Deps, name string, force bool) error {
	if force {
		return d.Manager.DeleteWithSnapshots(name)
	}
	return d.Manager.Delete(name)
}
