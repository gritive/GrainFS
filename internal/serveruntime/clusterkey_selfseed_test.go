package serveruntime

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDirEmptyStrict(t *testing.T) {
	dir := t.TempDir()
	sub := filepath.Join(dir, "meta")

	empty, err := dirEmptyStrict(sub) // absent
	require.NoError(t, err)
	require.True(t, empty)

	require.NoError(t, os.MkdirAll(sub, 0o700)) // present, empty
	empty, err = dirEmptyStrict(sub)
	require.NoError(t, err)
	require.True(t, empty)

	require.NoError(t, os.WriteFile(filepath.Join(sub, "x"), []byte("y"), 0o600)) // non-empty
	empty, err = dirEmptyStrict(sub)
	require.NoError(t, err)
	require.False(t, empty)
}

func TestFileAbsentStrict(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "cluster.id")

	absent, err := fileAbsentStrict(p)
	require.NoError(t, err)
	require.True(t, absent)

	require.NoError(t, os.WriteFile(p, []byte("z"), 0o600))
	absent, err = fileAbsentStrict(p)
	require.NoError(t, err)
	require.False(t, absent)
}

// On a non-ErrNotExist error (traversing through a regular file → ENOTDIR) the
// strict probes MUST surface the error, never report "absent"/"empty".
func TestStrictProbesFailClosedOnError(t *testing.T) {
	dir := t.TempDir()
	notADir := filepath.Join(dir, "afile")
	require.NoError(t, os.WriteFile(notADir, []byte("x"), 0o600))
	child := filepath.Join(notADir, "child") // path traverses a file

	_, err := fileAbsentStrict(child)
	require.Error(t, err)

	_, err = dirEmptyStrict(child)
	require.Error(t, err)
}

// kekDirEmptyStrict: missing → empty; has <N>.key → not empty; dir-is-a-file → error.
func TestKekDirEmptyStrict(t *testing.T) {
	dir := t.TempDir()

	empty, err := kekDirEmptyStrict(filepath.Join(dir, "keys")) // absent
	require.NoError(t, err)
	require.True(t, empty)

	keys := filepath.Join(dir, "keys")
	require.NoError(t, os.MkdirAll(keys, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(keys, "3.key"), []byte("k"), 0o600))
	empty, err = kekDirEmptyStrict(keys)
	require.NoError(t, err)
	require.False(t, empty)

	fileAsDir := filepath.Join(dir, "keysfile")
	require.NoError(t, os.WriteFile(fileAsDir, []byte("x"), 0o600))
	_, err = kekDirEmptyStrict(fileAsDir)
	require.Error(t, err)
}
