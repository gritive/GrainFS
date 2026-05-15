package admin

import "testing"

func TestRouteAvailabilityManifestDocumentsHiddenOptionalAdminFeatures(t *testing.T) {
	tests := []struct {
		feature routeFeature
		name    string
	}{
		{feature: routeFeatureBuckets, name: "buckets"},
		{feature: routeFeatureNfsExports, name: "nfs_exports"},
		{feature: routeFeatureIAM, name: "iam"},
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

func TestRouteFeatureRoutesVisibleRequiresDependencyForHiddenAdminFeatures(t *testing.T) {
	d := &Deps{}
	if routeFeatureRoutesVisible(d, routeFeatureBuckets) {
		t.Fatal("bucket routes should be hidden without bucket ops")
	}
	if routeFeatureRoutesVisible(d, routeFeatureNfsExports) {
		t.Fatal("NFS export routes should be hidden without export service")
	}
	if routeFeatureRoutesVisible(d, routeFeatureIAM) {
		t.Fatal("IAM routes should be hidden without IAM service")
	}
}
