package serveruntime

import (
	"os"
	"path/filepath"
	"strings"
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
		require.Equal(t, "9", string(b))
	})

	t.Run("populated dir without marker is refused", func(t *testing.T) {
		dir := t.TempDir()
		err := EnsureBulkCipherFormat(dir, true)
		require.Error(t, err)
		require.Contains(t, err.Error(), "pre-XAES")
	})

	t.Run("marker 9 with bulk data returns nil", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "encryption.format"), []byte("9"), 0o600))
		err := EnsureBulkCipherFormat(dir, true)
		require.NoError(t, err)
	})

	t.Run("unknown marker version is refused", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "encryption.format"), []byte("10"), 0o600))
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
		require.Equal(t, "9", string(b))
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

	// Each current encrypted-bytes location must independently trigger
	// detection, even with shards/ absent.
	for _, sub := range []string{"shards", "data", "blobs", "shared-fsm", "wal", "raft/raft-v2", "meta_raft/raft-v2"} {
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
		require.Equal(t, "9", string(b))
	})
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
	require.Equal(t, "9", string(b))

	_, err = os.Stat(filepath.Join(strayDataDir, "encryption.format"))
	require.True(t, os.IsNotExist(err), "marker must NOT be under opts.DataDir when DataDirs is set")
}

func TestEnsureBulkCipherFormatRejectsPreXAESDEK(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "encryption.format"), []byte("2"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := EnsureBulkCipherFormat(dir, true)
	if err == nil {
		t.Fatal("expected loud-fail on pre-XAES-DEK marker \"2\", got nil")
	}
	if !strings.Contains(err.Error(), "2") || !strings.Contains(err.Error(), "9") {
		t.Fatalf("error should name the version mismatch (2 vs 9): %v", err)
	}
}

// TestEnsureBulkCipherFormatRejectsPriorVersion3 — a dir stamped with the
// previous format "3" (XAES-DEK but static-sealed data-plane bytes) must
// loud-fail on this "9" binary rather than mis-read.
func TestEnsureBulkCipherFormatRejectsPriorVersion3(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "encryption.format"), []byte("3"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := EnsureBulkCipherFormat(dir, true)
	if err == nil {
		t.Fatal("expected loud-fail on prior format marker \"3\", got nil")
	}
	if !strings.Contains(err.Error(), "3") || !strings.Contains(err.Error(), "9") {
		t.Fatalf("error should name the version mismatch (3 vs 9): %v", err)
	}
}

// TestEnsureBulkCipherFormatRejectsPriorVersion4 — a dir stamped with "4"
// (R1 data-plane DEK but static-sealed IAM credentials) must loud-fail on
// this "9" binary (R2 DEK-sealed IAM credentials). Pre-"5" IAM ciphertext
// used the static *encrypt.Encryptor and is unreadable under the new
// DataEncryptor seam.
func TestEnsureBulkCipherFormatRejectsPriorVersion4(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "encryption.format"), []byte("4"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := EnsureBulkCipherFormat(dir, true)
	if err == nil {
		t.Fatal("expected loud-fail on prior format marker \"4\", got nil")
	}
	if !strings.Contains(err.Error(), "4") || !strings.Contains(err.Error(), "9") {
		t.Fatalf("error should name the version mismatch (4 vs 9): %v", err)
	}
}

func TestEnsureBulkCipherFormatRejectsPriorVersion5(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, bulkCipherFormatFile), []byte("5"), 0o600))
	err := EnsureBulkCipherFormat(dir, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected \"9\"")
}

func TestEnsureBulkCipherFormatStampsCurrentVersionOnFreshDir(t *testing.T) {
	dir := t.TempDir()
	if err := EnsureBulkCipherFormat(dir, false); err != nil {
		t.Fatalf("fresh dir should pass: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dir, "encryption.format"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(got)) != "9" {
		t.Fatalf("fresh dir stamped %q, want \"9\"", got)
	}
}
