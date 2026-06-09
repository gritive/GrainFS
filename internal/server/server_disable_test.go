package server

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/server/servertest"
	"github.com/gritive/GrainFS/internal/storage"
)

// TestWithIcebergDisabled_PreventsFallbackCatalog verifies that
// ensureRuntimeDefaults creates an icebergCatalog via fallback when no
// WithIcebergDisabled() option is set, making Iceberg routes available.
func TestWithIcebergDisabled_PreventsFallbackCatalog(t *testing.T) {
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })

	addr := fmt.Sprintf("127.0.0.1:%d", servertest.FreePort(t))
	srv := startTestServer(t, addr, backend)
	require.True(t, srv.routeFeatureAvailable(routeFeatureIceberg),
		"without disable, iceberg should be available via fallback catalog")
}

// TestWithIcebergDisabled_DisablesFeature verifies that WithIcebergDisabled()
// prevents ensureRuntimeDefaults from creating the fallback icebergCatalog,
// leaving the route unavailable.
func TestWithIcebergDisabled_DisablesFeature(t *testing.T) {
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })

	addr := fmt.Sprintf("127.0.0.1:%d", servertest.FreePort(t))
	srv := startTestServer(t, addr, backend, WithIcebergDisabled())
	require.False(t, srv.routeFeatureAvailable(routeFeatureIceberg),
		"WithIcebergDisabled must prevent fallback catalog creation")
}
