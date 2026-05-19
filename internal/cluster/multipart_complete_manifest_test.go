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

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage"
)

func uploadTestPart(t *testing.T, b *DistributedBackend, uploadID string, partNumber int, body []byte) storage.Part {
	t.Helper()
	part, err := b.UploadPart(context.Background(), "bucket", "mp.bin", uploadID, partNumber, bytes.NewReader(body))
	require.NoError(t, err)
	return *part
}

func TestBuildMultipartCompleteManifestRejectsDuplicatePartNumber(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "bucket"))
	up, err := b.CreateMultipartUpload(context.Background(), "bucket", "mp.bin", "application/octet-stream")
	require.NoError(t, err)
	p := uploadTestPart(t, b, up.UploadID, 1, bytes.Repeat([]byte("a"), 5<<20))

	_, err = b.buildMultipartCompleteManifest(up.UploadID, []storage.Part{p, p})
	require.ErrorIs(t, err, storage.ErrInvalidPart)
}

func TestBuildMultipartCompleteManifestRejectsETagMismatch(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "bucket"))
	up, err := b.CreateMultipartUpload(context.Background(), "bucket", "mp.bin", "application/octet-stream")
	require.NoError(t, err)
	p1 := uploadTestPart(t, b, up.UploadID, 1, bytes.Repeat([]byte("a"), 5<<20))
	p2 := uploadTestPart(t, b, up.UploadID, 2, []byte("tail"))
	p2.ETag = "not-the-real-etag"

	_, err = b.buildMultipartCompleteManifest(up.UploadID, []storage.Part{p1, p2})
	require.ErrorIs(t, err, storage.ErrInvalidPart)
}

func TestBuildMultipartCompleteManifestRejectsMissingPartFile(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "bucket"))
	up, err := b.CreateMultipartUpload(context.Background(), "bucket", "mp.bin", "application/octet-stream")
	require.NoError(t, err)
	p1 := uploadTestPart(t, b, up.UploadID, 1, bytes.Repeat([]byte("a"), 5<<20))
	p2 := uploadTestPart(t, b, up.UploadID, 2, []byte("tail"))
	require.NoError(t, os.Remove(b.partPath(up.UploadID, 2)))

	_, err = b.buildMultipartCompleteManifest(up.UploadID, []storage.Part{p1, p2})
	require.ErrorIs(t, err, storage.ErrInvalidPart)
}

func TestBuildMultipartCompleteManifestRejectsSmallNonFinalPart(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "bucket"))
	up, err := b.CreateMultipartUpload(context.Background(), "bucket", "mp.bin", "application/octet-stream")
	require.NoError(t, err)
	p1 := uploadTestPart(t, b, up.UploadID, 1, []byte("too-small"))
	p2 := uploadTestPart(t, b, up.UploadID, 2, []byte("tail"))

	_, err = b.buildMultipartCompleteManifest(up.UploadID, []storage.Part{p1, p2})
	require.ErrorIs(t, err, storage.ErrInvalidPart)
}

func TestBuildMultipartCompleteManifestReturnsSortedPartsAndTotalSize(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "bucket"))
	up, err := b.CreateMultipartUpload(context.Background(), "bucket", "mp.bin", "application/octet-stream")
	require.NoError(t, err)
	p1 := uploadTestPart(t, b, up.UploadID, 1, bytes.Repeat([]byte("a"), 5<<20))
	p2 := uploadTestPart(t, b, up.UploadID, 2, []byte("tail"))

	manifest, err := b.buildMultipartCompleteManifest(up.UploadID, []storage.Part{p2, p1})
	require.NoError(t, err)
	require.Equal(t, int64((5<<20)+4), manifest.TotalSize)
	require.Equal(t, []storage.MultipartPartEntry{
		{PartNumber: 1, Size: int64(5 << 20), ETag: p1.ETag},
		{PartNumber: 2, Size: 4, ETag: p2.ETag},
	}, manifest.Parts)
}

func TestMultipartCompleteManifestReaderStreamsInOrder(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "bucket"))
	up, err := b.CreateMultipartUpload(context.Background(), "bucket", "mp.bin", "application/octet-stream")
	require.NoError(t, err)
	p1Body := bytes.Repeat([]byte("a"), 5<<20)
	p2Body := []byte("tail")
	p1 := uploadTestPart(t, b, up.UploadID, 1, p1Body)
	p2 := uploadTestPart(t, b, up.UploadID, 2, p2Body)

	manifest, err := b.buildMultipartCompleteManifest(up.UploadID, []storage.Part{p1, p2})
	require.NoError(t, err)
	rc, err := manifest.Open()
	require.NoError(t, err)
	got, readErr := io.ReadAll(rc)
	closeErr := rc.Close()
	require.NoError(t, readErr)
	require.NoError(t, closeErr)
	require.Equal(t, append(append([]byte{}, p1Body...), p2Body...), got)
}

func TestMultipartCompleteManifestReaderPropagatesDeletedPartOnOpen(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "bucket"))
	up, err := b.CreateMultipartUpload(context.Background(), "bucket", "mp.bin", "application/octet-stream")
	require.NoError(t, err)
	p1 := uploadTestPart(t, b, up.UploadID, 1, bytes.Repeat([]byte("a"), 5<<20))
	p2 := uploadTestPart(t, b, up.UploadID, 2, []byte("tail"))

	manifest, err := b.buildMultipartCompleteManifest(up.UploadID, []storage.Part{p1, p2})
	require.NoError(t, err)
	require.NoError(t, os.Remove(b.partPath(up.UploadID, 2)))
	rc, err := manifest.Open()
	require.NoError(t, err)
	_, readErr := io.ReadAll(rc)
	closeErr := rc.Close()
	require.Error(t, readErr)
	require.NoError(t, closeErr)
}

func TestMultipartCompleteManifestEncryptedReaderReturnsPlaintext(t *testing.T) {
	b := newTestDistributedBackend(t)
	enc := testEncryptor(t)
	b.SetShardService(NewShardService(b.root, nil, WithEncryptor(enc)), []string{b.selfAddr})
	require.NoError(t, b.CreateBucket(context.Background(), "bucket"))
	up, err := b.CreateMultipartUpload(context.Background(), "bucket", "mp.bin", "application/octet-stream")
	require.NoError(t, err)
	p1Body := bytes.Repeat([]byte("a"), 5<<20)
	p2Body := []byte("tail")
	p1 := uploadTestPart(t, b, up.UploadID, 1, p1Body)
	p2 := uploadTestPart(t, b, up.UploadID, 2, p2Body)

	raw, err := os.ReadFile(b.partPath(up.UploadID, 1))
	require.NoError(t, err)
	require.NotContains(t, string(raw), string(p1Body[:64]))

	manifest, err := b.buildMultipartCompleteManifest(up.UploadID, []storage.Part{p1, p2})
	require.NoError(t, err)
	rc, err := manifest.Open()
	require.NoError(t, err)
	got, readErr := io.ReadAll(rc)
	closeErr := rc.Close()
	require.NoError(t, readErr)
	require.NoError(t, closeErr)
	require.Equal(t, append(append([]byte{}, p1Body...), p2Body...), got)
}

func TestMultipartCompleteManifestWholeMD5Helper(t *testing.T) {
	sum := md5.Sum([]byte("abc"))
	require.Equal(t, hex.EncodeToString(sum[:]), trimCompletePartETag(`"`+hex.EncodeToString(sum[:])+`"`))
	require.True(t, errors.Is(fmtInvalidPart("x"), storage.ErrInvalidPart))
}

func testEncryptor(t *testing.T) *encrypt.Encryptor {
	t.Helper()
	enc, err := encrypt.NewEncryptor(bytes.Repeat([]byte{0x45}, 32))
	require.NoError(t, err)
	return enc
}
