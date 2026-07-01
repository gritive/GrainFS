package server

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIcebergRemovedFromRouteSurface asserts /iceberg/ is gone from the route manifest.
func TestIcebergRemovedFromRouteSurface(t *testing.T) {
	for _, e := range routeSurfaceManifest {
		assert.False(t, strings.HasPrefix(e.pathPrefix, "/iceberg") || strings.HasPrefix(e.pathExact, "/iceberg") ||
			strings.HasPrefix(e.pathPrefix, "/_iceberg") || strings.HasPrefix(e.pathExact, "/_iceberg"),
			"route surface manifest must not contain iceberg paths, found: %+v", e)
	}
}

// TestIcebergRemovedFromRouteAvailability asserts iceberg is gone from availability manifest.
func TestIcebergRemovedFromRouteAvailability(t *testing.T) {
	for _, e := range routeAvailabilityManifest {
		require.NotEqual(t, "iceberg", e.name, "route availability manifest must not contain 'iceberg'")
	}
}
