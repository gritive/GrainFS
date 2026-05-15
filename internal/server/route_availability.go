package server

type routeFeature uint8

const (
	routeFeatureAlerts routeFeature = iota
	routeFeatureIncident
	routeFeatureReceipt
	routeFeatureAuditHealth
	routeFeatureAuditSearchS3
	routeFeatureEventLog
	routeFeatureBalancer
	routeFeatureScrubber
	routeFeatureRaftSnapshot
	routeFeatureSnapshot
	routeFeatureIceberg
	routeFeatureLifecycle
	routeFeatureCluster
	routeFeatureClusterJoin
	routeFeatureClusterMembership
)

type routeUnavailableMode uint8

const (
	routeHiddenWhenUnavailable routeUnavailableMode = iota
	routeRegisteredWhenUnavailable
)

type routeAvailabilityEntry struct {
	feature         routeFeature
	name            string
	unavailableMode routeUnavailableMode
}

var routeAvailabilityManifest = []routeAvailabilityEntry{
	{feature: routeFeatureAlerts, name: "alerts", unavailableMode: routeHiddenWhenUnavailable},
	{feature: routeFeatureIncident, name: "incident", unavailableMode: routeHiddenWhenUnavailable},
	{feature: routeFeatureReceipt, name: "receipt", unavailableMode: routeHiddenWhenUnavailable},
	{feature: routeFeatureAuditHealth, name: "audit_health", unavailableMode: routeRegisteredWhenUnavailable},
	{feature: routeFeatureAuditSearchS3, name: "audit_search_s3", unavailableMode: routeRegisteredWhenUnavailable},
	{feature: routeFeatureEventLog, name: "eventlog", unavailableMode: routeRegisteredWhenUnavailable},
	{feature: routeFeatureBalancer, name: "balancer", unavailableMode: routeRegisteredWhenUnavailable},
	{feature: routeFeatureScrubber, name: "scrubber", unavailableMode: routeRegisteredWhenUnavailable},
	{feature: routeFeatureRaftSnapshot, name: "raft_snapshot", unavailableMode: routeRegisteredWhenUnavailable},
	{feature: routeFeatureSnapshot, name: "snapshot", unavailableMode: routeRegisteredWhenUnavailable},
	{feature: routeFeatureIceberg, name: "iceberg", unavailableMode: routeRegisteredWhenUnavailable},
	{feature: routeFeatureLifecycle, name: "lifecycle", unavailableMode: routeRegisteredWhenUnavailable},
	{feature: routeFeatureCluster, name: "cluster", unavailableMode: routeRegisteredWhenUnavailable},
	{feature: routeFeatureClusterJoin, name: "cluster_join", unavailableMode: routeRegisteredWhenUnavailable},
	{feature: routeFeatureClusterMembership, name: "cluster_membership", unavailableMode: routeRegisteredWhenUnavailable},
}

func routeAvailabilityForFeature(feature routeFeature) routeAvailabilityEntry {
	for _, entry := range routeAvailabilityManifest {
		if entry.feature == feature {
			return entry
		}
	}
	return routeAvailabilityEntry{feature: feature, unavailableMode: routeHiddenWhenUnavailable}
}

func (s *Server) routeFeatureAvailable(feature routeFeature) bool {
	switch feature {
	case routeFeatureAlerts:
		return s.alerts != nil
	case routeFeatureIncident:
		return s.incidentStore != nil
	case routeFeatureReceipt:
		return s.receiptAPI != nil
	case routeFeatureAuditHealth:
		return s.auditOutbox != nil
	case routeFeatureAuditSearchS3:
		return s.auditSearcher != nil
	case routeFeatureEventLog:
		return s.evStore != nil
	case routeFeatureBalancer:
		return s.balancer != nil
	case routeFeatureScrubber:
		return s.scrubber != nil
	case routeFeatureRaftSnapshot:
		return s.raftSnapshots != nil
	case routeFeatureSnapshot:
		return s.snapMgr != nil
	case routeFeatureIceberg:
		return s.icebergCatalog != nil
	case routeFeatureLifecycle:
		return s.lifecycle != nil
	case routeFeatureCluster:
		return s.cluster != nil
	case routeFeatureClusterJoin:
		return s.joinCluster != nil
	case routeFeatureClusterMembership:
		return s.membership != nil
	default:
		return false
	}
}

func (s *Server) routeFeatureRoutesVisible(feature routeFeature) bool {
	if s.routeFeatureAvailable(feature) {
		return true
	}
	return routeAvailabilityForFeature(feature).unavailableMode != routeHiddenWhenUnavailable
}
