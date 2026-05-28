package serveruntime

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadStaticEncryptionKeyForRun_AllowsInviteJoinWithoutEncryptionKey(t *testing.T) {
	dir := t.TempDir()
	enc, raw, err := loadStaticEncryptionKeyForRun(
		ServeOptions{DataDir: dir, RaftAddr: "127.0.0.1:7000"},
		dir,
		&inviteJoinState{},
	)

	require.NoError(t, err)
	require.Nil(t, enc)
	require.Nil(t, raw)
}

func TestLoadStaticEncryptionKeyForRun_AllowsStaticKeylessMemberRestart(t *testing.T) {
	dir := t.TempDir()
	stageStaticKeylessMemberArtifacts(t, dir)

	enc, raw, err := loadStaticEncryptionKeyForRun(
		ServeOptions{DataDir: dir, RaftAddr: "127.0.0.1:7000"},
		dir,
		nil,
	)

	require.NoError(t, err)
	require.Nil(t, enc)
	require.Nil(t, raw)
}

func TestLoadStaticEncryptionKeyForRun_AllowsStaticKeylessMemberRestartWithFlagClusterKey(t *testing.T) {
	dir := t.TempDir()
	stageStaticKeylessMemberArtifacts(t, dir)
	require.NoError(t, os.Remove(filepath.Join(dir, "keys.d", "current.key")))

	enc, raw, err := loadStaticEncryptionKeyForRun(
		ServeOptions{DataDir: dir, RaftAddr: "127.0.0.1:7000", ClusterKey: "recovery-psk"},
		dir,
		nil,
	)

	require.NoError(t, err)
	require.Nil(t, enc)
	require.Nil(t, raw)
}

func TestLoadStaticEncryptionKeyForRun_RejectsPartialStaticKeylessArtifacts(t *testing.T) {
	dir := t.TempDir()
	p := inviteJoinPathsFor(dir)
	mustWrite(t, p.clusterID, []byte("0123456789abcdef"))
	mustWrite(t, filepath.Join(p.keysDir, "0.key"), bytes.Repeat([]byte{0x11}, 32))
	mustWrite(t, p.nodeKeyEnc, []byte("sealed"))
	mustWrite(t, filepath.Join(dir, "keys.d", nodeKeyGenFile), []byte("0"))

	_, _, err := loadStaticEncryptionKeyForRun(
		ServeOptions{DataDir: dir, RaftAddr: "127.0.0.1:7000"},
		dir,
		nil,
	)

	require.Error(t, err)
	require.Contains(t, err.Error(), "--encryption-key-file is required")
}

func TestLoadStaticEncryptionKeyForRun_StillRejectsClusterModeWithoutInviteJoin(t *testing.T) {
	dir := t.TempDir()
	_, _, err := loadStaticEncryptionKeyForRun(
		ServeOptions{DataDir: dir, RaftAddr: "127.0.0.1:7000"},
		dir,
		nil,
	)

	require.Error(t, err)
	require.Contains(t, err.Error(), "--encryption-key-file is required")
}

func TestLoadStaticEncryptionKeyForRun_LoadsLegacyInviteEncryptionKeyWhenPresent(t *testing.T) {
	dir := t.TempDir()
	key := bytes.Repeat([]byte{0x42}, 32)
	mustWrite(t, filepath.Join(dir, "encryption.key"), key)

	enc, raw, err := loadStaticEncryptionKeyForRun(
		ServeOptions{DataDir: dir, RaftAddr: "127.0.0.1:7000"},
		dir,
		&inviteJoinState{},
	)

	require.NoError(t, err)
	require.NotNil(t, enc)
	require.Equal(t, key, raw)
}

func stageStaticKeylessMemberArtifacts(t *testing.T, dir string) {
	t.Helper()
	p := inviteJoinPathsFor(dir)
	mustWrite(t, p.clusterID, []byte("0123456789abcdef"))
	mustWrite(t, filepath.Join(p.keysDir, "0.key"), bytes.Repeat([]byte{0x11}, 32))
	mustWrite(t, p.nodeKeyEnc, []byte("sealed"))
	mustWrite(t, filepath.Join(dir, "keys.d", nodeKeyGenFile), []byte("0"))
	mustWrite(t, p.currentKey, []byte("psk"))
}
