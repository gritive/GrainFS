package serveruntime

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/stretchr/testify/require"
)

func TestBuildKEKProtector_DefaultIsPlaintext(t *testing.T) {
	p, err := buildKEKProtector(Config{})
	require.NoError(t, err)
	require.Equal(t, "plaintext", p.Name())

	p2, err := buildKEKProtector(Config{KEKProtector: "plaintext"})
	require.NoError(t, err)
	require.Equal(t, "plaintext", p2.Name())
}

func TestBuildKEKProtector_EnvSelectsEnv(t *testing.T) {
	p, err := buildKEKProtector(Config{KEKProtector: "env"})
	require.NoError(t, err)
	require.Equal(t, "env", p.Name())
}

func TestBuildKEKProtector_UnknownErrors(t *testing.T) {
	_, err := buildKEKProtector(Config{KEKProtector: "bogus"})
	require.Error(t, err)
}

func TestResolveRecoverySecret_EnvBeatsFile(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, "secret")
	require.NoError(t, os.WriteFile(f, []byte("from-file\n"), 0o600))

	t.Setenv(kekRecoverySecretEnv, "from-env")
	secret, err := resolveRecoverySecret(Config{KEKRecoverySecretFile: f})
	require.NoError(t, err)
	require.Equal(t, []byte("from-env"), secret)
}

func TestResolveRecoverySecret_FileWhenNoEnv(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, "secret")
	require.NoError(t, os.WriteFile(f, []byte(" from-file \n"), 0o600))
	t.Setenv(kekRecoverySecretEnv, "")

	secret, err := resolveRecoverySecret(Config{KEKRecoverySecretFile: f})
	require.NoError(t, err)
	require.Equal(t, []byte("from-file"), secret, "file secret is trimmed of surrounding whitespace")
}

func TestResolveRecoverySecret_AbsentReturnsEmpty(t *testing.T) {
	t.Setenv(kekRecoverySecretEnv, "  ")
	secret, err := resolveRecoverySecret(Config{})
	require.NoError(t, err)
	require.Empty(t, secret, "whitespace/unset -> absent")
}

// End-to-end through the keystore: env protector created from config produces a
// container; plaintext default stays raw.
func TestBuildKEKProtector_EnvCreatesContainer(t *testing.T) {
	t.Setenv(kekRecoverySecretEnv, "boot-pass")
	dir := t.TempDir()
	keysDir := filepath.Join(dir, "keys")

	p, err := buildKEKProtector(Config{KEKProtector: "env"})
	require.NoError(t, err)
	_, err = encrypt.LoadOrInitKEKStoreDirWithProtector(keysDir, p)
	require.NoError(t, err)

	blob, err := os.ReadFile(filepath.Join(keysDir, "0.key"))
	require.NoError(t, err)
	require.True(t, encrypt.LooksLikeEnvKEK(blob), "env protector from config must write a container")
}
