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
