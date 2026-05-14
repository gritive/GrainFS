package cluster

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

type failingReader struct {
	err error
}

func (r failingReader) Read([]byte) (int, error) {
	return 0, r.err
}

func TestSpoolObjectComputesSizeAndETag(t *testing.T) {
	data := []byte("hello")
	sp, err := spoolObject(context.Background(), t.TempDir(), bytes.NewReader(data), "__grainfs_volumes")
	require.NoError(t, err)
	defer sp.Cleanup()
	require.Equal(t, int64(5), sp.Size)
	require.Equal(t, storage.InternalETag(data), sp.ETag) // xxhash3 for internal buckets

	rc, err := sp.Open()
	require.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, "hello", string(got))
}

func TestSpoolObjectS3BucketUsesMD5ETag(t *testing.T) {
	data := []byte("hello s3 object")
	sp, err := spoolObject(context.Background(), t.TempDir(), bytes.NewReader(data), "user-bucket")
	require.NoError(t, err)
	defer sp.Cleanup()
	require.Equal(t, int64(len(data)), sp.Size)
	h := md5.Sum(data)
	require.Equal(t, hex.EncodeToString(h[:]), sp.ETag) // MD5 for S3 user buckets
}

func TestSpoolObjectNoBucketSkipsHashing(t *testing.T) {
	data := []byte("no etag needed")
	sp, err := spoolObject(context.Background(), t.TempDir(), bytes.NewReader(data), "")
	require.NoError(t, err)
	defer sp.Cleanup()
	require.Equal(t, int64(len(data)), sp.Size)
	require.Empty(t, sp.ETag) // no bucket → no etag computed
}

func TestSpoolObjectCleansTempOnReadError(t *testing.T) {
	dir := t.TempDir()
	_, err := spoolObject(context.Background(), dir, failingReader{err: errors.New("boom")}, "__grainfs_volumes")
	require.ErrorContains(t, err, "spool object")
	entries, readErr := os.ReadDir(dir)
	require.NoError(t, readErr)
	require.Empty(t, entries)
}

func TestSpoolECShardsReconstructsOriginal(t *testing.T) {
	sp, err := spoolObject(context.Background(), t.TempDir(), bytes.NewReader([]byte("hello erasure coding")), "__grainfs_volumes")
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
	sp, err := spoolObject(context.Background(), t.TempDir(), bytes.NewReader(nil), "__grainfs_volumes")
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

func TestECStreamBlockSizeScalesWithObjectSize(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 1}

	require.Equal(t, 64<<10, ecStreamBlockSize(cfg, 64<<10))
	require.Equal(t, 1<<20, ecStreamBlockSize(cfg, 2<<20))
	require.Equal(t, 1<<20, ecStreamBlockSize(cfg, 64<<20))
}
