package admin

import (
	"context"
)

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

// AnnotateVolumeHealthForMetrics reuses the admin health composer for aggregate
// scrape-time metrics without re-listing volume metadata.
func AnnotateVolumeHealthForMetrics(ctx context.Context, d *Deps, vols []VolumeInfo) {
	fetchAndAnnotateHealth(ctx, d, vols)
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
