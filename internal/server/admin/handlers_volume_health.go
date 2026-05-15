package admin

import (
	"context"
	"encoding/json"

	"github.com/gritive/GrainFS/internal/incident"
)

func incidentToWireMap(st incident.IncidentState) map[string]any {
	var out map[string]any
	buf, err := json.Marshal(st)
	if err != nil {
		return map[string]any{"id": st.ID}
	}
	if err := json.Unmarshal(buf, &out); err != nil {
		return map[string]any{"id": st.ID}
	}
	return out
}

// fetchAndAnnotateHealth fetches active incident state plus per-volume
// replica/EC layout signals and delegates composition to the pure
// annotateVolumeHealth composer in health.go. On incident-fetch error it
// stamps "unknown"/"incident_lookup_failed" without invoking the composer.
// VolumePlacement errors are silent — the composer falls back to incident-only
// health rather than stamping a noisy "replica_lookup_failed" while clusters
// are reconfiguring.
func fetchAndAnnotateHealth(ctx context.Context, d *Deps, vols []VolumeInfo) {
	if d.Incident == nil {
		return
	}
	all, err := d.Incident.List(ctx, 500)
	if err != nil {
		for i := range vols {
			vols[i].Health = "unknown"
			vols[i].HealthReasons = []string{"incident_lookup_failed"}
		}
		return
	}
	replicas := fetchReplicaSummaries(ctx, d, vols)
	annotateVolumeHealth(vols, all, replicas)
}

// fetchReplicaSummaries calls the optional VolumePlacement source and returns
// nil on absence or error. Pulled into its own helper so handlers stay simple
// and the silent-on-error contract is named.
func fetchReplicaSummaries(ctx context.Context, d *Deps, vols []VolumeInfo) map[string]ReplicaLayoutFact {
	if d.VolumePlacement == nil {
		return nil
	}
	names := make([]string, len(vols))
	for i, v := range vols {
		names[i] = v.Name
	}
	replicas, err := d.VolumePlacement.VolumeReplicaSummaries(ctx, names)
	if err != nil {
		return nil
	}
	return replicas
}
