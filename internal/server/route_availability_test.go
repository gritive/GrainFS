package server

import (
	"testing"

	"github.com/gritive/GrainFS/internal/audit"
	"github.com/gritive/GrainFS/internal/eventstore"
	"github.com/gritive/GrainFS/internal/receipt"
)

func TestRouteAvailabilityManifestDocumentsHiddenOptionalFeatures(t *testing.T) {
	tests := []struct {
		feature routeFeature
		name    string
	}{
		{feature: routeFeatureAlerts, name: "alerts"},
		{feature: routeFeatureIncident, name: "incident"},
		{feature: routeFeatureReceipt, name: "receipt"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry := routeAvailabilityForFeature(tt.feature)
			if entry.name != tt.name {
				t.Fatalf("routeAvailabilityForFeature(%v).name = %q, want %q", tt.feature, entry.name, tt.name)
			}
			if entry.unavailableMode != routeHiddenWhenUnavailable {
				t.Fatalf("routeAvailabilityForFeature(%v).unavailableMode = %v, want hidden", tt.feature, entry.unavailableMode)
			}
		})
	}
}

func TestRouteAvailabilityManifestDocumentsRegisteredOptionalFeatures(t *testing.T) {
	tests := []struct {
		feature routeFeature
		name    string
	}{
		{feature: routeFeatureAuditHealth, name: "audit_health"},
		{feature: routeFeatureAuditSearchS3, name: "audit_search_s3"},
		{feature: routeFeatureEventLog, name: "eventlog"},
		{feature: routeFeatureBalancer, name: "balancer"},
		{feature: routeFeatureScrubber, name: "scrubber"},
		{feature: routeFeatureRaftSnapshot, name: "raft_snapshot"},
		{feature: routeFeatureSnapshot, name: "snapshot"},
		{feature: routeFeatureIceberg, name: "iceberg"},
		{feature: routeFeatureLifecycle, name: "lifecycle"},
		{feature: routeFeatureCluster, name: "cluster"},
		{feature: routeFeatureClusterJoin, name: "cluster_join"},
		{feature: routeFeatureClusterMembership, name: "cluster_membership"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry := routeAvailabilityForFeature(tt.feature)
			if entry.name != tt.name {
				t.Fatalf("routeAvailabilityForFeature(%v).name = %q, want %q", tt.feature, entry.name, tt.name)
			}
			if entry.unavailableMode != routeRegisteredWhenUnavailable {
				t.Fatalf("routeAvailabilityForFeature(%v).unavailableMode = %v, want registered", tt.feature, entry.unavailableMode)
			}
		})
	}
}

func TestRouteFeatureRoutesVisibleRequiresDependencyForHiddenFeatures(t *testing.T) {
	s := &Server{}
	if s.routeFeatureRoutesVisible(routeFeatureAlerts) {
		t.Fatal("alerts routes should be hidden without alerts state")
	}
	if s.routeFeatureRoutesVisible(routeFeatureIncident) {
		t.Fatal("incident routes should be hidden without incident store")
	}
	if s.routeFeatureRoutesVisible(routeFeatureReceipt) {
		t.Fatal("receipt routes should be hidden without receipt API")
	}
	if !s.routeFeatureRoutesVisible(routeFeatureAuditHealth) {
		t.Fatal("audit health route should remain visible without audit outbox")
	}
	if !s.routeFeatureRoutesVisible(routeFeatureAuditSearchS3) {
		t.Fatal("audit search route should remain visible without searcher")
	}
	if !s.routeFeatureRoutesVisible(routeFeatureEventLog) {
		t.Fatal("eventlog route should remain visible without event store")
	}
	if !s.routeFeatureRoutesVisible(routeFeatureBalancer) {
		t.Fatal("balancer route should remain visible without balancer")
	}
	if !s.routeFeatureRoutesVisible(routeFeatureScrubber) {
		t.Fatal("scrubber route should remain visible without scrubber")
	}
	if !s.routeFeatureRoutesVisible(routeFeatureRaftSnapshot) {
		t.Fatal("raft snapshot route should remain visible without snapshotter")
	}
	if !s.routeFeatureRoutesVisible(routeFeatureSnapshot) {
		t.Fatal("snapshot routes should remain visible without snapshot manager")
	}
	if !s.routeFeatureRoutesVisible(routeFeatureIceberg) {
		t.Fatal("iceberg routes should remain visible without catalog")
	}
	if !s.routeFeatureRoutesVisible(routeFeatureLifecycle) {
		t.Fatal("lifecycle routes should remain visible without lifecycle service")
	}
	if !s.routeFeatureRoutesVisible(routeFeatureCluster) {
		t.Fatal("cluster routes should remain visible without cluster runtime")
	}
	if !s.routeFeatureRoutesVisible(routeFeatureClusterJoin) {
		t.Fatal("cluster join route should remain visible without join function")
	}
	if !s.routeFeatureRoutesVisible(routeFeatureClusterMembership) {
		t.Fatal("cluster membership route should remain visible without membership controller")
	}
}

func TestRouteFeatureRoutesVisibleWhenDependencyExists(t *testing.T) {
	s := &Server{
		alerts:        &AlertsState{},
		incidentStore: &incidentStoreStub{},
		receiptAPI:    receipt.NewAPI(nil, nil, nil, 0),
		auditOutbox:   &audit.Outbox{},
		auditSearcher: &fakeAuditSearcher{},
		evStore:       &eventstore.Store{},
	}

	if !s.routeFeatureRoutesVisible(routeFeatureAlerts) {
		t.Fatal("alerts routes should be visible with alerts state")
	}
	if !s.routeFeatureRoutesVisible(routeFeatureIncident) {
		t.Fatal("incident routes should be visible with incident store")
	}
	if !s.routeFeatureRoutesVisible(routeFeatureReceipt) {
		t.Fatal("receipt routes should be visible with receipt API")
	}
	if !s.routeFeatureAvailable(routeFeatureAuditHealth) {
		t.Fatal("audit health should be available with audit outbox")
	}
	if !s.routeFeatureAvailable(routeFeatureAuditSearchS3) {
		t.Fatal("audit search should be available with searcher")
	}
	if !s.routeFeatureAvailable(routeFeatureEventLog) {
		t.Fatal("eventlog should be available with event store")
	}
}
