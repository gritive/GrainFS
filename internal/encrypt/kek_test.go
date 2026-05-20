package encrypt

import (
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadOrGenerateKEK_AutoGenerateOnMissing(t *testing.T) {
	tmp, err := os.CreateTemp(t.TempDir(), "kek-*.key")
	require.NoError(t, err)
	path := tmp.Name()
	require.NoError(t, tmp.Close())
	require.NoError(t, os.Remove(path))

	kek, err := LoadOrGenerateKEK("file://" + path)
	require.NoError(t, err)
	assert.Len(t, kek, KEKSize)

	info, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())
}

func TestLoadOrGenerateKEK_ReadExisting(t *testing.T) {
	want := make([]byte, KEKSize)
	for i := range want {
		want[i] = byte(i)
	}

	path := t.TempDir() + "/kek.key"
	require.NoError(t, os.WriteFile(path, want, 0o600))

	kek, err := LoadOrGenerateKEK("file://" + path)
	require.NoError(t, err)
	assert.Equal(t, want, kek)
}

func TestLoadOrGenerateKEK_RejectKMS(t *testing.T) {
	_, err := LoadOrGenerateKEK("kms://arn:aws:kms:us-east-1:123456789012:key/abc")
	assert.True(t, errors.Is(err, ErrUnsupportedKEKSource))
}

func TestLoadOrGenerateKEK_RejectsWrongSizeFile(t *testing.T) {
	for _, sz := range []int{0, 16, 31, 33, 64} {
		path := t.TempDir() + "/kek.key"
		require.NoError(t, os.WriteFile(path, make([]byte, sz), 0o600))
		_, err := LoadOrGenerateKEK("file://" + path)
		require.Errorf(t, err, "size %d must be rejected", sz)
		require.Contains(t, err.Error(), "32", "error must name expected size")
	}
}

func TestLoadOrGenerateKEK_RejectsRelativePath(t *testing.T) {
	_, err := LoadOrGenerateKEK("file://relative/kek.key")
	require.Error(t, err)
	require.Contains(t, err.Error(), "absolute")
}

func TestLoadOrGenerateKEK_RejectsLoosePermissions(t *testing.T) {
	for _, mode := range []os.FileMode{0o644, 0o640, 0o604, 0o660} {
		path := t.TempDir() + "/kek.key"
		require.NoError(t, os.WriteFile(path, make([]byte, KEKSize), mode))
		_, err := LoadOrGenerateKEK("file://" + path)
		require.ErrorIsf(t, err, ErrKEKPermissionsTooLoose, "mode %#o must be rejected", mode)
	}
}

// TestLoadKEK_StrictMissingReturnsErrNotFound is the §7 T57 (F#21) primitive:
// callers that must NOT auto-generate (joining nodes, post-Restore runtime)
// rely on LoadKEK returning ErrKEKNotFound to surface a remediation path.
func TestLoadKEK_StrictMissingReturnsErrNotFound(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/kek.key"
	_, err := LoadKEK("file://" + path)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrKEKNotFound)
	// The error must NOT have side-created the file (strict semantic).
	_, statErr := os.Stat(path)
	require.True(t, os.IsNotExist(statErr), "LoadKEK must not create the file")
}

func TestLoadKEK_ReadExisting(t *testing.T) {
	want := make([]byte, KEKSize)
	for i := range want {
		want[i] = byte(i)
	}
	path := t.TempDir() + "/kek.key"
	require.NoError(t, os.WriteFile(path, want, 0o600))

	kek, err := LoadKEK("file://" + path)
	require.NoError(t, err)
	assert.Equal(t, want, kek)
}

func TestLoadKEK_RejectKMS(t *testing.T) {
	_, err := LoadKEK("kms://arn:aws:kms:us-east-1:123456789012:key/abc")
	assert.True(t, errors.Is(err, ErrUnsupportedKEKSource))
}

func TestLoadOrGenerateKEK_RejectsSymlink(t *testing.T) {
	d := t.TempDir()
	real := d + "/real.key"
	require.NoError(t, os.WriteFile(real, make([]byte, KEKSize), 0o600))
	link := d + "/kek.key"
	require.NoError(t, os.Symlink(real, link))
	_, err := LoadOrGenerateKEK("file://" + link)
	require.ErrorIs(t, err, ErrKEKSymlink)
}
