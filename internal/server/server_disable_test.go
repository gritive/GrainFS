package server

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/server/servertest"
	"github.com/gritive/GrainFS/internal/storage"
)

// TestIceberg_AvailableWhenCatalogWired verifies that wiring an Iceberg catalog
// via WithIcebergCatalog makes the Iceberg routes available.
func TestIceberg_AvailableWhenCatalogWired(t *testing.T) {
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })

	addr := fmt.Sprintf("127.0.0.1:%d", servertest.FreePort(t))
	srv := startTestServer(t, addr, backend, WithIcebergCatalog(fakeIcebergCatalog{warehouse: "warehouse"}))
	require.True(t, srv.routeFeatureAvailable(routeFeatureIceberg),
		"iceberg should be available when a catalog is wired")
}

// TestIceberg_UnavailableWithoutCatalog verifies that, absent an explicitly
// wired catalog, the Iceberg routes are unavailable. No DBProvider fallback
// auto-creates a catalog (the dead escape hatch was removed), so a plain
// backend leaves the feature off; WithIcebergDisabled is the symmetric way to
// express that intent at the boot edge.
func TestIceberg_UnavailableWithoutCatalog(t *testing.T) {
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })

	addr := fmt.Sprintf("127.0.0.1:%d", servertest.FreePort(t))
	srv := startTestServer(t, addr, backend, WithIcebergDisabled())
	require.False(t, srv.routeFeatureAvailable(routeFeatureIceberg),
		"iceberg must be unavailable when no catalog is wired")
}
