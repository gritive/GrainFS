package serveruntime

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

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
