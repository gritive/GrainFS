package serveruntime

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/resourcewatch"
	"github.com/gritive/GrainFS/internal/storage"
)

func TestSetupVolumeRuntimeWiresManagerCacheOptionsAndDedupCleanup(t *testing.T) {
	backend, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	defer backend.Close()

	before := countRegisteredDBs(resourcewatch.DBCategoryDedup)
	runtime, err := SetupVolumeRuntime(context.Background(), VolumeRuntimeOptions{
		VolumeManagerOptions: VolumeManagerOptions{
			DedupEnabled:   true,
			BlockCacheSize: 1024,
		},
		DataDir: t.TempDir(),
		Backend: backend,
	})

	require.NoError(t, err)
	require.NotNil(t, runtime.Manager)
	require.NotNil(t, runtime.BlockCache)
	require.NotEmpty(t, runtime.ServerOptions)
	require.Equal(t, before+1, countRegisteredDBs(resourcewatch.DBCategoryDedup))
	require.NoError(t, runtime.Close())
	require.Equal(t, before, countRegisteredDBs(resourcewatch.DBCategoryDedup))
	require.NoError(t, runtime.Close())
}

func TestSetupVolumeRuntimeDisablesOptionalDedupRole(t *testing.T) {
	dataFile := filepath.Join(t.TempDir(), "not-a-directory")
	require.NoError(t, os.WriteFile(dataFile, []byte("x"), 0o644))
	backend, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	defer backend.Close()

	before := countRegisteredDBs(resourcewatch.DBCategoryDedup)
	runtime, err := SetupVolumeRuntime(context.Background(), VolumeRuntimeOptions{
		VolumeManagerOptions: VolumeManagerOptions{DedupEnabled: true},
		DataDir:              dataFile,
		Backend:              backend,
	})

	require.NoError(t, err)
	require.NotNil(t, runtime.Manager)
	require.NotNil(t, runtime.BlockCache)
	require.NotEmpty(t, runtime.ServerOptions)
	require.Equal(t, before, countRegisteredDBs(resourcewatch.DBCategoryDedup))
	require.NoError(t, runtime.Close())
}

func countRegisteredDBs(category resourcewatch.Category) int {
	count := 0
	for _, entry := range resourcewatch.Default.Snapshot() {
		if entry.Category == category {
			count++
		}
	}
	return count
}
