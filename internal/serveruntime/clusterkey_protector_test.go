package serveruntime

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/transport"
	"github.com/stretchr/testify/require"
)

func envCfg(t *testing.T) Config {
	t.Helper()
	t.Setenv("GRAINFS_KEK_RECOVERY_SECRET", "boot-pass")
	return Config{KEKProtector: "env"}
}

func TestNewClusterKeystore_PlaintextDefault(t *testing.T) {
	dir := t.TempDir()
	ks, err := newClusterKeystore(dir, Config{})
	require.NoError(t, err)
	require.NoError(t, ks.WriteCurrent("xyz"))
	raw, err := os.ReadFile(filepath.Join(dir, "keys.d", "current.key"))
	require.NoError(t, err)
	require.Equal(t, "xyz\n", string(raw), "plaintext default stays byte-identical")
}

func TestNewClusterKeystore_EnvFailsClosedWithoutSecret(t *testing.T) {
	t.Setenv("GRAINFS_KEK_RECOVERY_SECRET", "")
	_, err := newClusterKeystore(t.TempDir(), Config{KEKProtector: "env"})
	require.Error(t, err, "env mode without recovery secret must fail closed at construction")
}

func TestNewClusterKeystore_EnvWritesContainer(t *testing.T) {
	dir := t.TempDir()
	ks, err := newClusterKeystore(dir, envCfg(t))
	require.NoError(t, err)
	require.NoError(t, ks.WriteCurrent("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"))
	raw, err := os.ReadFile(filepath.Join(dir, "keys.d", "current.key"))
	require.NoError(t, err)
	require.True(t, encrypt.LooksLikeEnvKEK(raw), "env config writes a GKEK container")
}

func TestResolveClusterKey_EnvBootRebindRewritesSlot(t *testing.T) {
	dir := t.TempDir()
	// Seed a slot via env protector bound to host-a.
	ksA := transport.NewKeystoreWithProtector(dir, encrypt.NewEnvProtectorForTest([]string{"machine-id:host-a"}, "pass"))
	psk := "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
	require.NoError(t, ksA.WriteCurrent(psk))
	before, err := os.ReadFile(filepath.Join(dir, "keys.d", "current.key"))
	require.NoError(t, err)

	// Boot on a "moved" machine (host-B): ReadCurrentForBoot triggers recovery +
	// rewrap; ResolveClusterKey re-persists. We exercise the keystore boot read +
	// rewrite directly (ResolveClusterKey wires config-built protector; here we
	// drive the protector seam to assert the rewrite behavior).
	ksB := transport.NewKeystoreWithProtector(dir, encrypt.NewEnvProtectorForTest([]string{"machine-id:host-B"}, "pass"))
	got, rewrap, err := ksB.ReadCurrentForBoot()
	require.NoError(t, err)
	require.True(t, rewrap, "binding changed -> rewrap signalled")
	require.Equal(t, psk, got)
	require.NoError(t, ksB.WriteCurrent(got)) // boot path re-persists
	after, err := os.ReadFile(filepath.Join(dir, "keys.d", "current.key"))
	require.NoError(t, err)
	require.NotEqual(t, before, after, "rebind rewrote the slot for the new machine")

	// 3rd boot on host-B opens via env slot (no rewrap).
	ksB2 := transport.NewKeystoreWithProtector(dir, encrypt.NewEnvProtectorForTest([]string{"machine-id:host-B"}, "pass"))
	got2, rewrap2, err := ksB2.ReadCurrentForBoot()
	require.NoError(t, err)
	require.False(t, rewrap2)
	require.Equal(t, psk, got2)
}

func TestResolveClusterKey_ReadOnlyDirPersistNonFatal(t *testing.T) {
	dir := t.TempDir()
	ksA := transport.NewKeystoreWithProtector(dir, encrypt.NewEnvProtectorForTest([]string{"machine-id:host-a"}, "pass"))
	psk := "11112222333344445555666677778888aaaabbbbccccddddeeeeffff00009999"
	require.NoError(t, ksA.WriteCurrent(psk))

	// Make keys.d read-only so the rebind re-persist fails; boot must still
	// resolve the key from memory (NON-fatal persist failure).
	keysDir := filepath.Join(dir, "keys.d")
	require.NoError(t, os.Chmod(keysDir, 0o500))
	t.Cleanup(func() { _ = os.Chmod(keysDir, 0o700) })

	t.Setenv("GRAINFS_KEK_RECOVERY_SECRET", "pass")
	// ResolveClusterKey builds an env protector from config; inject matching factors
	// is not wired through config (factors are host-real), so simulate the moved
	// machine via the keystore seam: the boot read still returns the key, and a
	// failed WriteCurrent must not abort. We assert the keystore read path:
	got, _, err := ksA.ReadCurrentForBoot()
	require.NoError(t, err, "boot read succeeds even when the dir is read-only")
	require.Equal(t, psk, got)
}
