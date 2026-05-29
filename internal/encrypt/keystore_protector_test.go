package encrypt

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// envProtectorForTest builds an EnvProtector bound to a fixed factor set and
// passphrase, with a deterministic salt, for keystore integration tests.
func envProtectorForTest(factors []string, secret string) *EnvProtector {
	return &EnvProtector{
		factors:  fakeFactors{f: factors},
		recovery: func() ([]byte, error) { return []byte(secret), nil },
		saltGen:  randomSalt,
	}
}

func TestKEKStore_PlaintextDefault_ByteIdentical(t *testing.T) {
	dir := t.TempDir()
	keysDir := filepath.Join(dir, "keys")
	s, err := LoadOrInitKEKStoreDir(keysDir)
	require.NoError(t, err)
	require.Equal(t, "plaintext", s.protector.Name())

	// The default path must leave 0.key as exactly the raw 32-byte KEK.
	raw, err := os.ReadFile(filepath.Join(keysDir, "0.key"))
	require.NoError(t, err)
	require.Len(t, raw, KEKSize, "plaintext default must keep <V>.key byte-identical (32 bytes)")

	kek, err := s.ActiveKEK()
	require.NoError(t, err)
	require.Equal(t, raw, kek)
}

func TestKEKStore_EnvProtector_CreateReloadStable(t *testing.T) {
	dir := t.TempDir()
	keysDir := filepath.Join(dir, "keys")
	factors := []string{"machine-id:host-a"}

	s1, err := LoadOrInitKEKStoreDirWithProtector(keysDir, envProtectorForTest(factors, "pass"))
	require.NoError(t, err)
	kek1, err := s1.ActiveKEK()
	require.NoError(t, err)

	// On disk it must be a container, not raw.
	blob, err := os.ReadFile(filepath.Join(keysDir, "0.key"))
	require.NoError(t, err)
	require.True(t, LooksLikeEnvKEK(blob), "env protector must store a GKEK container")
	require.NotEqual(t, kek1, blob)

	// Reload with same factors -> same KEK.
	s2, err := LoadOrInitKEKStoreDirWithProtector(keysDir, envProtectorForTest(factors, "pass"))
	require.NoError(t, err)
	kek2, err := s2.ActiveKEK()
	require.NoError(t, err)
	require.Equal(t, kek1, kek2)
}

func TestKEKStore_EnvProtector_RotationWrapsAllVersions(t *testing.T) {
	dir := t.TempDir()
	keysDir := filepath.Join(dir, "keys")
	factors := []string{"machine-id:host-a"}

	s, err := LoadOrInitKEKStoreDirWithProtector(keysDir, envProtectorForTest(factors, "pass"))
	require.NoError(t, err)

	// Simulate KEK rotation: AddAndPersist v1, v2 (the FSM rotation path).
	k1 := make([]byte, KEKSize)
	k1[0] = 1
	k2 := make([]byte, KEKSize)
	k2[0] = 2
	require.NoError(t, s.AddAndPersist(keysDir, 1, k1))
	require.NoError(t, s.AddAndPersist(keysDir, 2, k2))

	for _, v := range []uint32{0, 1, 2} {
		blob, err := os.ReadFile(filepath.Join(keysDir, fileNameFor(v)))
		require.NoError(t, err)
		require.True(t, LooksLikeEnvKEK(blob), "v%d must be a container (uniform on-disk format)", v)
	}

	// Reload restores all three versions, each unwrapped correctly.
	s2, err := LoadOrInitKEKStoreDirWithProtector(keysDir, envProtectorForTest(factors, "pass"))
	require.NoError(t, err)
	require.ElementsMatch(t, []uint32{0, 1, 2}, s2.Versions())
	got1, err := s2.Get(1)
	require.NoError(t, err)
	require.Equal(t, k1, got1)
	got2, err := s2.Get(2)
	require.NoError(t, err)
	require.Equal(t, k2, got2)
}

func TestKEKStore_EnvProtector_FactorChangeRebindsFile(t *testing.T) {
	dir := t.TempDir()
	keysDir := filepath.Join(dir, "keys")

	s1, err := LoadOrInitKEKStoreDirWithProtector(keysDir, envProtectorForTest([]string{"machine-id:host-a"}, "pass"))
	require.NoError(t, err)
	kek1, err := s1.ActiveKEK()
	require.NoError(t, err)
	before, err := os.ReadFile(filepath.Join(keysDir, "0.key"))
	require.NoError(t, err)

	// Reload on a "moved" machine: recovery path + rebind rewrites the file.
	s2, err := LoadOrInitKEKStoreDirWithProtector(keysDir, envProtectorForTest([]string{"machine-id:host-B"}, "pass"))
	require.NoError(t, err)
	kek2, err := s2.ActiveKEK()
	require.NoError(t, err)
	require.Equal(t, kek1, kek2, "recovery preserves the KEK")

	after, err := os.ReadFile(filepath.Join(keysDir, "0.key"))
	require.NoError(t, err)
	require.NotEqual(t, before, after, "rebind must rewrite the container for the new factors")

	// Third reload on host-B opens via env slot (rebind worked).
	s3, err := LoadOrInitKEKStoreDirWithProtector(keysDir, envProtectorForTest([]string{"machine-id:host-B"}, "pass"))
	require.NoError(t, err)
	kek3, err := s3.ActiveKEK()
	require.NoError(t, err)
	require.Equal(t, kek1, kek3)
}

func TestKEKStore_EnvProtector_FailClosed_NoRegeneration(t *testing.T) {
	dir := t.TempDir()
	keysDir := filepath.Join(dir, "keys")

	_, err := LoadOrInitKEKStoreDirWithProtector(keysDir, envProtectorForTest([]string{"machine-id:host-a"}, "pass"))
	require.NoError(t, err)
	before, err := os.ReadFile(filepath.Join(keysDir, "0.key"))
	require.NoError(t, err)

	// Moved machine AND wrong passphrase -> must error, must NOT regenerate.
	_, err = LoadOrInitKEKStoreDirWithProtector(keysDir, envProtectorForTest([]string{"machine-id:host-B"}, "WRONG"))
	require.Error(t, err, "both env + recovery fail -> fail-closed")

	after, err := os.ReadFile(filepath.Join(keysDir, "0.key"))
	require.NoError(t, err)
	require.Equal(t, before, after, "fail-closed must not overwrite/regenerate the key file")
}

func TestKEKStore_EnvProtector_LegacyRawMigrates(t *testing.T) {
	dir := t.TempDir()
	keysDir := filepath.Join(dir, "keys")
	require.NoError(t, os.MkdirAll(keysDir, 0o700))

	// Plant a legacy raw 32-byte 0.key (as the plaintext path would write).
	raw := make([]byte, KEKSize)
	raw[0] = 0xAA
	require.NoError(t, writeKEKFileAtomic(filepath.Join(keysDir, "0.key"), raw))

	s, err := LoadOrInitKEKStoreDirWithProtector(keysDir, envProtectorForTest([]string{"machine-id:host-a"}, "pass"))
	require.NoError(t, err)
	got, err := s.Get(0)
	require.NoError(t, err)
	require.Equal(t, raw, got, "legacy KEK preserved through migration")

	blob, err := os.ReadFile(filepath.Join(keysDir, "0.key"))
	require.NoError(t, err)
	require.True(t, LooksLikeEnvKEK(blob), "legacy file migrated to a container")
}

func TestKEKStore_EnvProtector_MixedStoreLoads(t *testing.T) {
	// Crash-during-migration: 0.key already a container, 1.key still raw.
	dir := t.TempDir()
	keysDir := filepath.Join(dir, "keys")
	factors := []string{"machine-id:host-a"}

	s, err := LoadOrInitKEKStoreDirWithProtector(keysDir, envProtectorForTest(factors, "pass"))
	require.NoError(t, err) // writes 0.key as container

	rawV1 := make([]byte, KEKSize)
	rawV1[0] = 0xBB
	require.NoError(t, writeKEKFileAtomic(filepath.Join(keysDir, "1.key"), rawV1)) // raw legacy v1

	s2, err := LoadOrInitKEKStoreDirWithProtector(keysDir, envProtectorForTest(factors, "pass"))
	require.NoError(t, err)
	require.ElementsMatch(t, []uint32{0, 1}, s2.Versions())
	gotV1, err := s2.Get(1)
	require.NoError(t, err)
	require.Equal(t, rawV1, gotV1)

	_ = s // silence
}

func TestWriteKEKFileAtomic_StaleTmpDoesNotWedge(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "0.key")
	// Simulate a crashed prior write leaving an orphaned tmp.
	require.NoError(t, os.WriteFile(path+".tmp", []byte("stale"), 0o600))

	blob := make([]byte, KEKSize)
	blob[0] = 0x77
	require.NoError(t, writeKEKFileAtomic(path, blob), "stale .tmp must not wedge the O_EXCL write")
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, blob, got)
}

func TestKEKStore_TruncatedPlaintextFileRejected(t *testing.T) {
	dir := t.TempDir()
	keysDir := filepath.Join(dir, "keys")
	require.NoError(t, os.MkdirAll(keysDir, 0o700))
	// A 10-byte file (truncated) under the default plaintext protector must be
	// rejected by Add (len != KEKSize), not silently accepted.
	f := filepath.Join(keysDir, "0.key")
	require.NoError(t, os.WriteFile(f, make([]byte, 10), 0o600))
	_, err := LoadOrInitKEKStoreDir(keysDir)
	require.Error(t, err, "truncated plaintext KEK must fail loud")
}
