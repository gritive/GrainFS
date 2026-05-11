package volume

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBadgerSnapshotStoreCreateRollback(t *testing.T) {
	mgr := setupDedupManager(t)
	const name = "rb"
	_, err := mgr.Create(name, 64*1024)
	require.NoError(t, err)
	_, err = mgr.WriteAt(name, bytes.Repeat([]byte{0xAA}, DefaultBlockSize), 0)
	require.NoError(t, err)
	snap1, err := mgr.CreateSnapshot(name)
	require.NoError(t, err)

	_, err = mgr.WriteAt(name, bytes.Repeat([]byte{0xBB}, DefaultBlockSize), 0)
	require.NoError(t, err)
	require.NoError(t, mgr.Rollback(name, snap1))

	buf := make([]byte, DefaultBlockSize)
	_, err = mgr.ReadAt(name, buf, 0)
	require.NoError(t, err)
	require.Equal(t, byte(0xAA), buf[0])
}
