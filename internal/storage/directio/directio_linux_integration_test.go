//go:build linux && directio_integration

package directio

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLinuxDirectIOIntegration_RoundTrip(t *testing.T) {
	dir := os.Getenv("GRAINFS_DIRECTIO_TEST_DIR")
	if dir == "" {
		dir = t.TempDir()
	} else {
		require.NoError(t, os.MkdirAll(dir, 0o755))
	}

	path := filepath.Join(dir, "directio-round-trip")
	payload := bytes.Repeat([]byte("grainfs-directio-"), 513)

	f, err := OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	require.NoError(t, err, "directio integration target must support Linux O_DIRECT")

	buf, alignedLen := AlignedCopy(payload)
	require.Equal(t, 0, alignedLen%PageSize())

	n, err := f.Write(buf)
	require.NoError(t, err)
	require.Equal(t, alignedLen, n)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Truncate(int64(len(payload))))
	require.NoError(t, f.Close())

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, payload, got)
}
