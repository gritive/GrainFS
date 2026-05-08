package admin

import (
	"strings"

	"github.com/gritive/GrainFS/internal/incident"
	"github.com/gritive/GrainFS/internal/volume"
)

// ReplicaLayoutFact is a per-volume signal of replica/EC layout vs the
// desired durability policy (ADR 0007). The first slice keeps it as an
// empty placeholder; a follow-up adapter will populate it from the
// object-index actual layout for each volume's block prefix.
type ReplicaLayoutFact struct{}

// annotateVolumeHealth fills Health and HealthReasons on each VolumeInfo by
// composing active incident scope matching with replica/EC layout signals.
// It performs no I/O: callers pre-fetch incidents and replica facts and pass
// them in. nil replicas means the replica signal source is currently
// unavailable; the first slice ignores it.
func annotateVolumeHealth(volumes []VolumeInfo, incidents []incident.IncidentState, replicas map[string]ReplicaLayoutFact) {
	for i := range volumes {
		applyHealthFromIncidents(&volumes[i], incidents)
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
	rank := map[string]int{"ok": 0, "warning": 1, "critical": 2, "unknown": 3}
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
