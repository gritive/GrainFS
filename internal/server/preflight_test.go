package server

import (
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunSystemPreflight_HappyPath(t *testing.T) {
	dir := t.TempDir()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	l.Close()

	err = RunSystemPreflight(PreflightConfig{
		DataDir:  dir,
		HTTPAddr: addr,
		NoAuth:   false,
	})
	assert.NoError(t, err)
}

func TestCheckDataDir_NotWritable(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("running as root, permission checks are bypassed")
	}
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o555))
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })

	err := checkDataDir(filepath.Join(dir, "sub"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "preflight:")
	assert.Contains(t, err.Error(), "Recovery guide")
}

func TestCheckPortFree_Occupied(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()

	err = checkPortFree(l.Addr().String())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "preflight:")
	assert.Contains(t, err.Error(), "Recovery guide")
}

func TestCheckPortFree_Empty(t *testing.T) {
	assert.NoError(t, checkPortFree(""))
}

func TestFmtBytes(t *testing.T) {
	assert.Equal(t, "512.0 MiB", fmtBytes(512<<20))
	assert.Equal(t, "1.0 GiB", fmtBytes(1<<30))
	assert.Equal(t, "500 KiB", fmtBytes(500<<10))
}
