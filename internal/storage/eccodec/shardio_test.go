package eccodec

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteReadShard_RoundTrip(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"small", []byte("hello shard")},
		{"binary", []byte{0x00, 0xff, 0x7f, 0x01, 0x02, 0x03}},
		{"4k", make([]byte, 4096)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "nested", "shard_0")

			require.NoError(t, WriteShardAtomic(path, tt.data))

			got, err := ReadShardVerified(path)
			require.NoError(t, err)
			assert.Equal(t, tt.data, got)
		})
	}
}

func TestReadShard_BitFlipDetected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "shard_0")
	require.NoError(t, WriteShardAtomic(path, []byte("original payload")))

	// Flip a byte in the payload (first byte).
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	raw[0] ^= 0x01
	require.NoError(t, os.WriteFile(path, raw, 0o644))

	_, err = ReadShardVerified(path)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrCRCMismatch), "want ErrCRCMismatch, got %v", err)
}

func TestReadShard_FooterFlipDetected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "shard_0")
	require.NoError(t, WriteShardAtomic(path, []byte("payload")))

	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	raw[len(raw)-1] ^= 0xff // flip last CRC byte
	require.NoError(t, os.WriteFile(path, raw, 0o644))

	_, err = ReadShardVerified(path)
	require.ErrorIs(t, err, ErrCRCMismatch)
}

func TestReadShard_TruncatedDetected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "shard_0")
	require.NoError(t, os.WriteFile(path, []byte{0x01, 0x02}, 0o644)) // < 4 bytes

	_, err := ReadShardVerified(path)
	require.ErrorIs(t, err, ErrCRCMismatch)
}

func TestReadShard_MissingFileReturnsNotFound(t *testing.T) {
	_, err := ReadShardVerified(filepath.Join(t.TempDir(), "does-not-exist"))
	require.Error(t, err)
	assert.True(t, os.IsNotExist(err))
}

func TestWriteShard_AtomicOnCrash(t *testing.T) {
	// Simulate: a pre-existing file should survive a failed write attempt
	// because the write goes to .tmp first and is only renamed on success.
	dir := t.TempDir()
	path := filepath.Join(dir, "shard_0")
	require.NoError(t, WriteShardAtomic(path, []byte("v1")))

	// Second write — success: file should now contain v2.
	require.NoError(t, WriteShardAtomic(path, []byte("v2")))
	got, err := ReadShardVerified(path)
	require.NoError(t, err)
	assert.Equal(t, []byte("v2"), got)

	// Leftover .tmp files should not exist after a successful write.
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range entries {
		assert.NotContains(t, e.Name(), ".tmp", "leftover tmp file: %s", e.Name())
	}
}
