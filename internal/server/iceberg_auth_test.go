package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestIcebergRoute_AnonymousModeRemoved_NoBypass asserts that NO iceberg
// route surface entry uses routeAuthnAnonymous. Regression guard: any future
// commit that flips an iceberg prefix back to anonymous fails CI.
func TestIcebergRoute_AnonymousModeRemoved_NoBypass(t *testing.T) {
	for _, entry := range routeSurfaceManifest {
		if entry.surface == routeSurfaceIceberg {
			assert.NotEqualf(t, routeAuthnAnonymous, entry.authn,
				"iceberg route %q/%q must not be anonymous",
				entry.pathPrefix, entry.pathExact)
		}
	}
}
