package server

import "testing"

func TestRouteAuthnPolicyForPath_DocumentsCurrentBypassSurface(t *testing.T) {
	tests := []struct {
		path string
		want routeAuthnPolicy
	}{
		{path: "/metrics", want: routeAuthnAnonymous},
		{path: "/ui/", want: routeAuthnAnonymous},
		{path: "/ui/api/volumes", want: routeAuthnAnonymous},
		{path: "/api/events", want: routeAuthnAnonymous},
		{path: "/api/events/heal/stream", want: routeAuthnAnonymous},
		{path: "/api/eventlog", want: routeAuthnAnonymous},
		{path: "/api/incidents", want: routeAuthnAnonymous},
		{path: "/api/incidents/open", want: routeAuthnAnonymous},
		{path: "/api/cluster/status", want: routeAuthnAnonymous},
		{path: "/api/cluster/remove-peer", want: routeAuthnAnonymous},
		{path: "/api/audit/health", want: routeAuthnAnonymous},
		{path: "/api/audit/s3", want: routeAuthnAnonymous},
		{path: "/api/cache/status", want: routeAuthnAnonymous},
		{path: "/iceberg/v1/config", want: routeAuthnSigV4},
		{path: "/_iceberg/v1/config", want: routeAuthnSigV4},
		{path: "/admin/health/raft", want: routeAuthnLocalhost},
		{path: "/bucket", want: routeAuthnSigV4},
		{path: "/bucket/key", want: routeAuthnSigV4},
		{path: "/api/admin/config", want: routeAuthnSigV4},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			if got := routeAuthnPolicyForPath(tt.path); got != tt.want {
				t.Fatalf("routeAuthnPolicyForPath(%q) = %v, want %v", tt.path, got, tt.want)
			}
		})
	}
}

func TestRouteSurfaceForPath_DocumentsS3AndNonS3Surfaces(t *testing.T) {
	tests := []struct {
		path string
		want routeSurface
	}{
		{path: "/", want: routeSurfaceS3},
		{path: "/bucket", want: routeSurfaceS3},
		{path: "/bucket/key", want: routeSurfaceS3},
		{path: "/metrics", want: routeSurfaceOps},
		{path: "/api/events", want: routeSurfaceOps},
		{path: "/api/foo", want: routeSurfaceOps},
		{path: "/api/admin/config", want: routeSurfaceOps},
		{path: "/admin/health/raft", want: routeSurfaceAdmin},
		{path: "/iceberg/v1/config", want: routeSurfaceIceberg},
		{path: "/_iceberg/v1/config", want: routeSurfaceIceberg},
		{path: "/ui/", want: routeSurfaceUI},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			if got := routeSurfaceForPath(tt.path); got != tt.want {
				t.Fatalf("routeSurfaceForPath(%q) = %v, want %v", tt.path, got, tt.want)
			}
		})
	}
}

func TestS3PathBucketKey_UsesRouteSurface(t *testing.T) {
	tests := []struct {
		path       string
		wantBucket string
		wantKey    string
	}{
		{path: "/", wantBucket: "", wantKey: ""},
		{path: "/bucket", wantBucket: "bucket", wantKey: ""},
		{path: "/bucket/key", wantBucket: "bucket", wantKey: "key"},
		{path: "/bucket/nested/key", wantBucket: "bucket", wantKey: "nested/key"},
		{path: "/api/foo", wantBucket: "", wantKey: ""},
		{path: "/api/admin/config", wantBucket: "", wantKey: ""},
		{path: "/admin/health/raft", wantBucket: "", wantKey: ""},
		{path: "/iceberg/v1/config", wantBucket: "", wantKey: ""},
		{path: "/_iceberg/v1/config", wantBucket: "", wantKey: ""},
		{path: "/ui/api/volumes", wantBucket: "", wantKey: ""},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			gotBucket, gotKey := s3PathBucketKey(tt.path)
			if gotBucket != tt.wantBucket || gotKey != tt.wantKey {
				t.Fatalf("s3PathBucketKey(%q) = (%q, %q), want (%q, %q)", tt.path, gotBucket, gotKey, tt.wantBucket, tt.wantKey)
			}
		})
	}
}

func TestRouteSkipsS3Authz_DoesNotTreatEveryAPIPathAsOps(t *testing.T) {
	tests := []struct {
		path string
		want bool
	}{
		{path: "/", want: true},
		{path: "/metrics", want: true},
		{path: "/ui/", want: true},
		{path: "/iceberg/v1/config", want: true},
		{path: "/_iceberg/v1/config", want: true},
		{path: "/admin/health/raft", want: true},
		{path: "/api/foo", want: false},
		{path: "/bucket/key", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			if got := routeSkipsS3Authz(tt.path); got != tt.want {
				t.Fatalf("routeSkipsS3Authz(%q) = %v, want %v", tt.path, got, tt.want)
			}
		})
	}
}

func TestRouteIsUISurface(t *testing.T) {
	tests := []struct {
		path string
		want bool
	}{
		{path: "/ui", want: true},
		{path: "/ui/", want: true},
		{path: "/ui/api/storage/protocols", want: true},
		{path: "/uiish", want: false},
		{path: "/api/ui", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			if got := routeIsUISurface(tt.path); got != tt.want {
				t.Fatalf("routeIsUISurface(%q) = %v, want %v", tt.path, got, tt.want)
			}
		})
	}
}
