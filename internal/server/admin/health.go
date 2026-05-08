package admin

import (
	"strings"

	"github.com/gritive/GrainFS/internal/incident"
	"github.com/gritive/GrainFS/internal/volume"
)

// ReplicaLayoutFact is a per-volume aggregate of object-version layout
// states for the volume's block-objects, classified against the desired
// EC profile (ADR 0007). Counts mirror the LayoutState enum in
// internal/cluster/topology_policy.go. A zero value contributes nothing
// to volume health.
type ReplicaLayoutFact struct {
	CurrentCount          int // matches desired EC profile
	PendingUpgradeCount   int // below desired (durability intact, profile lower)
	DowngradeSkippedCount int // above desired (acceptable, no action)
	RepairNeededCount     int // missing or wrong shard targets — durability gap
	UnknownCount          int // unresolved layout
}

// annotateVolumeHealth fills Health and HealthReasons on each VolumeInfo by
// composing active incident scope matching with replica/EC layout signals.
// It performs no I/O: callers pre-fetch incidents and replica facts and pass
// them in. nil replicas (or a missing volume name in the map) skips the
// replica contribution while keeping incident-only behavior.
func annotateVolumeHealth(volumes []VolumeInfo, incidents []incident.IncidentState, replicas map[string]ReplicaLayoutFact) {
	for i := range volumes {
		applyHealthFromIncidents(&volumes[i], incidents)
		if fact, ok := replicas[volumes[i].Name]; ok {
			applyHealthFromReplicas(&volumes[i], fact)
		}
	}
}

// applyHealthFromReplicas folds replica/EC layout counts into the volume's
// Health and HealthReasons. Repair-needed objects raise health to critical;
// pending-upgrade raises to degraded; unknown layout raises to warning.
// Downgrade-skipped and current counts contribute nothing because higher-
// or matching-profile objects need no operator action.
func applyHealthFromReplicas(info *VolumeInfo, fact ReplicaLayoutFact) {
	if fact.RepairNeededCount > 0 {
		info.Health = worseVolumeHealth(info.Health, "critical")
		info.HealthReasons = appendReason(info.HealthReasons, "replica_repair_needed")
	}
	if fact.PendingUpgradeCount > 0 {
		info.Health = worseVolumeHealth(info.Health, "degraded")
		info.HealthReasons = appendReason(info.HealthReasons, "replica_missing")
	}
	if fact.UnknownCount > 0 {
		info.Health = worseVolumeHealth(info.Health, "warning")
		info.HealthReasons = appendReason(info.HealthReasons, "replica_layout_unknown")
	}
}

func applyHealthFromIncidents(info *VolumeInfo, incidents []incident.IncidentState) {
	info.Health = "ok"
	info.HealthReasons = []string{}
	blockPrefix := volume.BlockKeyPrefix(info.Name)
	for _, st := range incidents {
		if !incidentMatchesVolume(st, info.Name, blockPrefix) || incidentResolved(st) {
			continue
		}
		info.Health = worseVolumeHealth(info.Health, healthForIncident(st))
		info.HealthReasons = appendReason(info.HealthReasons, "recent_incident")
	}
}

func incidentMatchesVolume(st incident.IncidentState, name, blockPrefix string) bool {
	if st.Scope.Bucket != volume.VolumeBucketName {
		return false
	}
	return st.Scope.Key == name || strings.HasPrefix(st.Scope.Key, blockPrefix)
}

func incidentResolved(st incident.IncidentState) bool {
	return st.State == incident.StateFixed || st.State == incident.StateIsolated
}

func healthForIncident(st incident.IncidentState) string {
	if st.Severity == incident.SeverityCritical ||
		st.State == incident.StateBlocked ||
		st.State == incident.StateNeedsHuman ||
		st.State == incident.StateProofUnavailable {
		return "critical"
	}
	return "warning"
}

func worseVolumeHealth(a, b string) string {
	rank := map[string]int{"ok": 0, "warning": 1, "degraded": 2, "critical": 3, "unknown": 4}
	if rank[b] > rank[a] {
		return b
	}
	return a
}

func appendReason(reasons []string, reason string) []string {
	for _, existing := range reasons {
		if existing == reason {
			return reasons
		}
	}
	return append(reasons, reason)
}
