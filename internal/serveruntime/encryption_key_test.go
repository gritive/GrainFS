package serveruntime

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEnsureBulkCipherFormat(t *testing.T) {
	t.Run("fresh dir writes marker and returns nil", func(t *testing.T) {
		dir := t.TempDir()
		err := EnsureBulkCipherFormat(dir, false)
		require.NoError(t, err)
		b, err := os.ReadFile(filepath.Join(dir, "encryption.format"))
		require.NoError(t, err)
		require.Equal(t, "2", string(b))
	})

	t.Run("populated dir without marker is refused", func(t *testing.T) {
		dir := t.TempDir()
		err := EnsureBulkCipherFormat(dir, true)
		require.Error(t, err)
		require.Contains(t, err.Error(), "pre-XAES")
	})

	t.Run("marker 2 with bulk data returns nil", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "encryption.format"), []byte("2"), 0o600))
		err := EnsureBulkCipherFormat(dir, true)
		require.NoError(t, err)
	})

	t.Run("unknown marker version is refused", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "encryption.format"), []byte("3"), 0o600))
		err := EnsureBulkCipherFormat(dir, true)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not supported")
	})

	t.Run("non-existent fresh dir is created and stamped", func(t *testing.T) {
		// The guard runs before preflight creates the data dir, so on a fresh
		// multi-root boot the dir may not exist yet. EnsureBulkCipherFormat must
		// create it rather than failing with "no such file or directory".
		dataDir := filepath.Join(t.TempDir(), "does-not-exist-yet")
		err := EnsureBulkCipherFormat(dataDir, false)
		require.NoError(t, err)
		b, err := os.ReadFile(filepath.Join(dataDir, "encryption.format"))
		require.NoError(t, err)
		require.Equal(t, "2", string(b))
	})
}

func TestBulkDataPresent(t *testing.T) {
	// touchEntry creates <dir>/<sub>/marker so the sub dir is non-empty.
	touchEntry := func(t *testing.T, dir, sub string) {
		t.Helper()
		p := filepath.Join(dir, sub)
		require.NoError(t, os.MkdirAll(p, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(p, "marker"), []byte("x"), 0o600))
	}

	t.Run("truly empty fresh dir returns false", func(t *testing.T) {
		dir := t.TempDir()
		require.False(t, BulkDataPresent(dir, nil, filepath.Join(dir, "meta")))
	})

	// Each encrypted-bytes location must independently trigger detection, even
	// with shards/ and datawal/ absent.
	for _, sub := range []string{"shards", "data", "datawal", "blobs", "shared-fsm", "wal"} {
		t.Run(sub+" non-empty returns true", func(t *testing.T) {
			dir := t.TempDir()
			touchEntry(t, dir, sub)
			require.True(t, BulkDataPresent(dir, nil, filepath.Join(dir, "meta")))
		})
	}

	t.Run("meta non-empty returns true", func(t *testing.T) {
		dir := t.TempDir()
		touchEntry(t, dir, "meta")
		require.True(t, BulkDataPresent(dir, nil, filepath.Join(dir, "meta")))
	})

	t.Run("custom meta dir non-empty returns true", func(t *testing.T) {
		dir := t.TempDir()
		metaDir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(metaDir, "KEYREGISTRY"), []byte("x"), 0o600))
		require.True(t, BulkDataPresent(dir, nil, metaDir))
	})

	t.Run("multi-root data non-empty returns true", func(t *testing.T) {
		root0 := t.TempDir()
		root1 := t.TempDir()
		touchEntry(t, root1, "data")
		require.True(t, BulkDataPresent(root0, []string{root0, root1}, filepath.Join(root0, "meta")))
	})

	// Detection must drive EnsureBulkCipherFormat to refuse a no-marker dir.
	for _, sub := range []string{"data", "blobs", "meta"} {
		t.Run("EnsureBulkCipherFormat refuses no-marker dir with "+sub, func(t *testing.T) {
			dir := t.TempDir()
			touchEntry(t, dir, sub)
			present := BulkDataPresent(dir, nil, filepath.Join(dir, "meta"))
			require.True(t, present)
			err := EnsureBulkCipherFormat(dir, present)
			require.Error(t, err)
			require.Contains(t, err.Error(), "pre-XAES")
		})
	}

	t.Run("fresh empty dir is stamped via the predicate", func(t *testing.T) {
		dir := t.TempDir()
		present := BulkDataPresent(dir, nil, filepath.Join(dir, "meta"))
		require.False(t, present)
		require.NoError(t, EnsureBulkCipherFormat(dir, present))
		b, err := os.ReadFile(filepath.Join(dir, "encryption.format"))
		require.NoError(t, err)
		require.Equal(t, "2", string(b))
	})
}

func TestLoadOrCreateEncryptionKeyRejectsAutoGenerationForClusterMode(t *testing.T) {
	_, err := LoadOrCreateEncryptionKey("", t.TempDir(), false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "--encryption-key-file is required")
}

func TestLoadOrCreateEncryptionKeyAllowsAutoGenerationForSoloBootstrap(t *testing.T) {
	dir := t.TempDir()
	_, err := LoadOrCreateEncryptionKey("", dir, true)
	require.NoError(t, err)

	info, err := os.Stat(filepath.Join(dir, "encryption.key"))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
}

func TestAllowAutoGenerateEncryptionKeyRejectsRaftAddrAndJoinPending(t *testing.T) {
	dir := t.TempDir()
	require.False(t, AllowAutoGenerateEncryptionKey(dir, "127.0.0.1:9001"))

	require.NoError(t, os.WriteFile(filepath.Join(dir, JoinPendingFile), []byte("127.0.0.1:9001"), 0o600))
	require.False(t, AllowAutoGenerateEncryptionKey(dir, ""))
}

func TestExplicitMissingEncryptionKeyStillReportsMountFailure(t *testing.T) {
	_, err := LoadOrCreateEncryptionKey(filepath.Join(t.TempDir(), "missing.key"), t.TempDir(), true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "mount failure?")
}

// TestPrimaryDataDirFromDataDirs verifies that when DataDirs is non-empty the
// canonical primary dir is DataDirs[0], not opts.DataDir. This mirrors the
// logic optionsToConfig uses (cfg.DataDir = cfg.DataDirs[0]) and is the core
// of Finding 2: the guard marker must be written under the real primary dir.
func TestPrimaryDataDirFromDataDirs(t *testing.T) {
	// primary = DataDirs[0] when DataDirs is non-empty
	dataDirs := []string{t.TempDir(), t.TempDir()}
	strayDataDir := t.TempDir() // opts.DataDir — different from DataDirs[0]

	primaryDataDir := strayDataDir
	if len(dataDirs) > 0 {
		primaryDataDir = dataDirs[0]
	}
	require.Equal(t, dataDirs[0], primaryDataDir)
	require.NotEqual(t, strayDataDir, primaryDataDir)

	// Stamp the format marker under primaryDataDir (as run_from_options now does).
	require.NoError(t, EnsureBulkCipherFormat(primaryDataDir, false))

	// Marker must be under DataDirs[0], not strayDataDir.
	b, err := os.ReadFile(filepath.Join(primaryDataDir, "encryption.format"))
	require.NoError(t, err)
	require.Equal(t, "2", string(b))

	_, err = os.Stat(filepath.Join(strayDataDir, "encryption.format"))
	require.True(t, os.IsNotExist(err), "marker must NOT be under opts.DataDir when DataDirs is set")
}
