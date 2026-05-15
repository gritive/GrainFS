package admin

import (
	"context"
	"errors"
	"fmt"

	"github.com/gritive/GrainFS/internal/adminapi"
	"github.com/gritive/GrainFS/internal/volume"
)

type StatResp = adminapi.StatResp

// StatVolume returns volume metadata plus recent incidents scoped to this volume.
// Scrub status is intentionally omitted in Phase B (deferred to follow-up since
// volume blocks bypass the EC scrub path; see TODOS.md).
func StatVolume(ctx context.Context, d *Deps, name string) (StatResp, error) {
	v, err := d.Manager.Get(name)
	if errors.Is(err, volume.ErrNotFound) {
		return StatResp{}, NewNotFound(fmt.Sprintf("volume %q not found", name))
	}
	if err != nil {
		return StatResp{}, NewInternal(err.Error())
	}
	resp := StatResp{Volume: toVolumeInfo(v)}
	if d.Incident != nil {
		all, lerr := d.Incident.List(ctx, 500)
		if lerr == nil {
			blockPrefix := volume.BlockKeyPrefix(name)
			for _, st := range all {
				if !incidentMatchesVolume(st, name, blockPrefix) {
					continue
				}
				resp.RecentIncidents = append(resp.RecentIncidents, incidentToWireMap(st))
				if len(resp.RecentIncidents) >= 50 {
					break
				}
			}
			vols := []VolumeInfo{resp.Volume}
			replicas := fetchReplicaSummaries(ctx, d, vols)
			annotateVolumeHealth(vols, all, replicas)
			resp.Volume = vols[0]
		} else {
			resp.Volume.Health = "unknown"
			resp.Volume.HealthReasons = []string{"incident_lookup_failed"}
		}
	}
	return resp, nil
}
