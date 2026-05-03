package cluster

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

type failingReader struct {
	err error
}

func (r failingReader) Read([]byte) (int, error) {
	return 0, r.err
}

func TestSpoolObjectComputesSizeAndETag(t *testing.T) {
	sp, err := spoolObject(context.Background(), t.TempDir(), bytes.NewReader([]byte("hello")), true)
	require.NoError(t, err)
	defer sp.Cleanup()
	require.Equal(t, int64(5), sp.Size)
	require.Equal(t, "5d41402abc4b2a76b9719d911017c592", sp.ETag)

	rc, err := sp.Open()
	require.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, "hello", string(got))
}

func TestSpoolObjectCleansTempOnReadError(t *testing.T) {
	dir := t.TempDir()
	_, err := spoolObject(context.Background(), dir, failingReader{err: errors.New("boom")}, true)
	require.ErrorContains(t, err, "spool object")
	entries, readErr := os.ReadDir(dir)
	require.NoError(t, readErr)
	require.Empty(t, entries)
}

func TestSpoolECShardsReconstructsOriginal(t *testing.T) {
	sp, err := spoolObject(context.Background(), t.TempDir(), bytes.NewReader([]byte("hello erasure coding")), true)
	require.NoError(t, err)
	defer sp.Cleanup()

	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	shards, err := spoolECShards(context.Background(), cfg, t.TempDir(), sp)
	require.NoError(t, err)
	defer shards.Cleanup()

	payloads := make([][]byte, cfg.NumShards())
	for i := range payloads {
		rc, err := shards.OpenShard(i)
		require.NoError(t, err)
		payloads[i], err = io.ReadAll(rc)
		require.NoError(t, err)
		require.NoError(t, rc.Close())
	}
	got, err := ECReconstruct(cfg, payloads)
	require.NoError(t, err)
	require.Equal(t, "hello erasure coding", string(got))
}

func TestSpoolECShardsReconstructsEmptyObject(t *testing.T) {
	sp, err := spoolObject(context.Background(), t.TempDir(), bytes.NewReader(nil), true)
	require.NoError(t, err)
	defer sp.Cleanup()

	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	shards, err := spoolECShards(context.Background(), cfg, t.TempDir(), sp)
	require.NoError(t, err)
	defer shards.Cleanup()

	payloads := make([][]byte, cfg.NumShards())
	for i := range payloads {
		rc, err := shards.OpenShard(i)
		require.NoError(t, err)
		payloads[i], err = io.ReadAll(rc)
		require.NoError(t, err)
		require.NoError(t, rc.Close())
	}
	got, err := ECReconstruct(cfg, payloads)
	require.NoError(t, err)
	require.Empty(t, got)
}
