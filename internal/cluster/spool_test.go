package cluster

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/gritive/GrainFS/internal/encrypt"
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

func TestEncryptedSpoolObjectHidesPlaintext(t *testing.T) {
	enc := newClusterTestEncryptor(t)
	payload := []byte("sensitive cluster spool payload")
	sp, err := spoolObjectEncrypted(context.Background(), t.TempDir(), bytes.NewReader(payload), "user-bucket", enc, "cluster-spool:test")
	require.NoError(t, err)
	defer sp.Cleanup()
	require.Equal(t, int64(len(payload)), sp.Size)
	require.Equal(t, "7d0467b8ee0ad76a1c41e37b3c2d3056", sp.ETag)

	raw, err := os.ReadFile(sp.Path)
	require.NoError(t, err)
	require.NotContains(t, string(raw), string(payload))

	rc, err := sp.Open()
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	require.Equal(t, payload, got)
}

func TestEncryptedSpoolObjectOpenStreamsWithoutDecryptingFutureRecords(t *testing.T) {
	enc := newClusterTestEncryptor(t)
	payload := append(bytes.Repeat([]byte("a"), spoolCopyBufferSize), bytes.Repeat([]byte("b"), spoolCopyBufferSize)...)
	sp, err := spoolObjectEncrypted(context.Background(), t.TempDir(), bytes.NewReader(payload), "user-bucket", enc, "cluster-spool:stream")
	require.NoError(t, err)
	defer sp.Cleanup()

	f, err := os.OpenFile(sp.Path, os.O_RDWR, 0)
	require.NoError(t, err)
	var hdr [8]byte
	_, err = f.ReadAt(hdr[:], 0)
	require.NoError(t, err)
	firstBlobLen := binary.BigEndian.Uint32(hdr[4:])
	secondBodyOffset := int64(8 + int(firstBlobLen) + 8)
	_, err = f.Seek(secondBodyOffset, io.SeekStart)
	require.NoError(t, err)
	_, err = f.Write([]byte{0x00})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	rc, err := sp.Open()
	require.NoError(t, err)
	defer rc.Close()
	buf := make([]byte, 32)
	n, err := rc.Read(buf)
	require.NoError(t, err)
	require.Equal(t, len(buf), n)
	require.Equal(t, bytes.Repeat([]byte("a"), len(buf)), buf)
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

func TestEncryptedSpoolECShardsHidePlaintextAndReconstruct(t *testing.T) {
	enc := newClusterTestEncryptor(t)
	marker := []byte("sensitive-erasure-coding-block-")
	payload := bytes.Repeat(marker, 4096)
	sp, err := spoolObjectEncrypted(context.Background(), t.TempDir(), bytes.NewReader(payload), "user-bucket", enc, "cluster-spool:test-ec")
	require.NoError(t, err)
	defer sp.Cleanup()

	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	shards, err := spoolECShards(context.Background(), cfg, t.TempDir(), sp)
	require.NoError(t, err)
	defer shards.Cleanup()

	payloads := make([][]byte, cfg.NumShards())
	for i := range payloads {
		raw, err := os.ReadFile(shards.paths[i])
		require.NoError(t, err)
		require.False(t, bytes.Contains(raw, marker), "raw EC shard %d contains plaintext marker", i)

		rc, err := shards.OpenShard(i)
		require.NoError(t, err)
		payloads[i], err = io.ReadAll(rc)
		require.NoError(t, err)
		require.NoError(t, rc.Close())
	}
	got, err := ECReconstruct(cfg, payloads)
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

func TestECStreamBlockSizeScalesWithObjectSize(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 1}

	require.Equal(t, 64<<10, ecStreamBlockSize(cfg, 64<<10))
	require.Equal(t, 1<<20, ecStreamBlockSize(cfg, 2<<20))
	require.Equal(t, 1<<20, ecStreamBlockSize(cfg, 64<<20))
}

func newClusterTestEncryptor(t *testing.T) *encrypt.Encryptor {
	t.Helper()
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	enc, err := encrypt.NewEncryptor(key)
	require.NoError(t, err)
	return enc
}
