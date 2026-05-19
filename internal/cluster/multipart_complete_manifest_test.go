package cluster

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"runtime"
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

func TestBuildMultipartCompleteManifestRejectsEmptyRequest(t *testing.T) {
	b := newTestDistributedBackend(t)

	_, err := b.buildMultipartCompleteManifest("upload-id", nil)
	require.ErrorIs(t, err, storage.ErrInvalidPart)
}

func TestBuildMultipartCompleteManifestRejectsInvalidPartNumber(t *testing.T) {
	b := newTestDistributedBackend(t)

	_, err := b.buildMultipartCompleteManifest("upload-id", []storage.Part{{PartNumber: 0, ETag: "etag"}})
	require.ErrorIs(t, err, storage.ErrInvalidPart)
}

func TestBuildMultipartCompleteManifestRejectsPartNumberAboveS3Limit(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "bucket"))
	up, err := b.CreateMultipartUpload(context.Background(), "bucket", "mp.bin", "application/octet-stream")
	require.NoError(t, err)
	p := uploadTestPart(t, b, up.UploadID, s3MultipartMaxPartNumber+1, []byte("tail"))

	_, err = b.buildMultipartCompleteManifest(up.UploadID, []storage.Part{p})
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

func TestBuildMultipartCompleteManifestRejectsUnreadablePartFile(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("chmod-based unreadable file test is not deterministic on windows")
	}
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "bucket"))
	up, err := b.CreateMultipartUpload(context.Background(), "bucket", "mp.bin", "application/octet-stream")
	require.NoError(t, err)
	p := uploadTestPart(t, b, up.UploadID, 1, bytes.Repeat([]byte("a"), 5<<20))
	partPath := b.partPath(up.UploadID, 1)
	require.NoError(t, os.Chmod(partPath, 0))
	t.Cleanup(func() {
		_ = os.Chmod(partPath, 0o600)
	})

	_, err = b.buildMultipartCompleteManifest(up.UploadID, []storage.Part{p})
	if err == nil {
		t.Skip("chmod-based unreadable file test is not deterministic for this user/platform")
	}
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
	require.Equal(t, up.UploadID, manifest.UploadID)
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

func TestMultipartCompleteManifestOpenReturnsFreshReaders(t *testing.T) {
	manifest := multipartCompleteManifest{
		Parts: []storage.MultipartPartEntry{
			{PartNumber: 1, Size: 3, ETag: "etag-1"},
			{PartNumber: 2, Size: 3, ETag: "etag-2"},
		},
		openPartFn: func(partNumber int) (io.ReadCloser, error) {
			switch partNumber {
			case 1:
				return io.NopCloser(bytes.NewReader([]byte("one"))), nil
			case 2:
				return io.NopCloser(bytes.NewReader([]byte("two"))), nil
			default:
				t.Fatalf("unexpected part number %d", partNumber)
				return nil, nil
			}
		},
	}

	first, err := manifest.Open()
	require.NoError(t, err)
	second, err := manifest.Open()
	require.NoError(t, err)
	firstBody, firstReadErr := io.ReadAll(first)
	secondBody, secondReadErr := io.ReadAll(second)
	require.NoError(t, firstReadErr)
	require.NoError(t, secondReadErr)
	require.NoError(t, first.Close())
	require.NoError(t, second.Close())
	require.Equal(t, []byte("onetwo"), firstBody)
	require.Equal(t, []byte("onetwo"), secondBody)
}

func TestMultipartCompleteManifestReaderOpensPartsLazily(t *testing.T) {
	var opened []int
	manifest := multipartCompleteManifest{
		Parts: []storage.MultipartPartEntry{
			{PartNumber: 1, Size: 2, ETag: "etag-1"},
			{PartNumber: 2, Size: 2, ETag: "etag-2"},
		},
		openPartFn: func(partNumber int) (io.ReadCloser, error) {
			opened = append(opened, partNumber)
			return io.NopCloser(bytes.NewReader([]byte("ab"))), nil
		},
	}

	rc, err := manifest.Open()
	require.NoError(t, err)
	require.Empty(t, opened)

	buf := make([]byte, 1)
	n, err := rc.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, []int{1}, opened)
	require.NoError(t, rc.Close())
}

func TestMultipartCompleteManifestReaderClosesEachPartOnce(t *testing.T) {
	var parts []*countingReadCloser
	manifest := multipartCompleteManifest{
		Parts: []storage.MultipartPartEntry{
			{PartNumber: 1, Size: 3, ETag: "etag-1"},
			{PartNumber: 2, Size: 3, ETag: "etag-2"},
		},
		openPartFn: func(partNumber int) (io.ReadCloser, error) {
			rc := &countingReadCloser{Reader: bytes.NewReader([]byte{byte('a' + partNumber - 1), byte('a' + partNumber - 1), byte('a' + partNumber - 1)})}
			parts = append(parts, rc)
			return rc, nil
		},
	}
	rc, err := manifest.Open()
	require.NoError(t, err)

	got, readErr := io.ReadAll(rc)
	closeErr := rc.Close()
	require.NoError(t, readErr)
	require.NoError(t, closeErr)
	require.Equal(t, []byte("aaabbb"), got)
	require.Len(t, parts, 2)
	require.Equal(t, 1, parts[0].closeCount)
	require.Equal(t, 1, parts[1].closeCount)
}

func TestMultipartCompleteManifestReaderReadAfterCloseReturnsClosedPipe(t *testing.T) {
	manifest := multipartCompleteManifest{
		Parts: []storage.MultipartPartEntry{{PartNumber: 1, Size: 3, ETag: "etag"}},
		openPartFn: func(partNumber int) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader([]byte("abc"))), nil
		},
	}
	rc, err := manifest.Open()
	require.NoError(t, err)
	require.NoError(t, rc.Close())

	n, err := rc.Read(make([]byte, 1))
	require.Zero(t, n)
	require.ErrorIs(t, err, io.ErrClosedPipe)
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

func TestMultipartCompleteManifestReaderPropagatesReadError(t *testing.T) {
	readErr := errors.New("read failed")
	manifest := multipartCompleteManifest{
		Parts: []storage.MultipartPartEntry{{PartNumber: 1, Size: 3, ETag: "etag"}},
		openPartFn: func(partNumber int) (io.ReadCloser, error) {
			require.Equal(t, 1, partNumber)
			return &errorReadCloser{err: readErr}, nil
		},
	}
	rc, err := manifest.Open()
	require.NoError(t, err)

	buf := make([]byte, 8)
	n, err := rc.Read(buf)
	require.Zero(t, n)
	require.ErrorIs(t, err, readErr)
	require.NoError(t, rc.Close())
}

func TestMultipartCompleteManifestReaderDefersCloseErrorAfterReturningBytes(t *testing.T) {
	closeErr := errors.New("close failed")
	manifest := multipartCompleteManifest{
		Parts: []storage.MultipartPartEntry{{PartNumber: 1, Size: 3, ETag: "etag"}},
		openPartFn: func(partNumber int) (io.ReadCloser, error) {
			require.Equal(t, 1, partNumber)
			return &bytesEOFReadCloser{body: []byte("abc"), closeErr: closeErr}, nil
		},
	}
	rc, err := manifest.Open()
	require.NoError(t, err)

	buf := make([]byte, 8)
	n, err := rc.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 3, n)
	require.Equal(t, "abc", string(buf[:n]))

	n, err = rc.Read(buf)
	require.Zero(t, n)
	require.ErrorIs(t, err, closeErr)
	require.NoError(t, rc.Close())
}

func TestMultipartCompleteManifestReaderCloseReturnsPendingCloseError(t *testing.T) {
	closeErr := errors.New("close failed")
	manifest := multipartCompleteManifest{
		Parts: []storage.MultipartPartEntry{{PartNumber: 1, Size: 3, ETag: "etag"}},
		openPartFn: func(partNumber int) (io.ReadCloser, error) {
			require.Equal(t, 1, partNumber)
			return &bytesEOFReadCloser{body: []byte("abc"), closeErr: closeErr}, nil
		},
	}
	rc, err := manifest.Open()
	require.NoError(t, err)

	buf := make([]byte, 8)
	n, err := rc.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 3, n)
	require.Equal(t, "abc", string(buf[:n]))

	require.ErrorIs(t, rc.Close(), closeErr)
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

type bytesEOFReadCloser struct {
	body     []byte
	closeErr error
	read     bool
}

func (r *bytesEOFReadCloser) Read(p []byte) (int, error) {
	if r.read {
		return 0, io.EOF
	}
	r.read = true
	return copy(p, r.body), io.EOF
}

func (r *bytesEOFReadCloser) Close() error {
	return r.closeErr
}

type countingReadCloser struct {
	*bytes.Reader
	closeCount int
}

func (r *countingReadCloser) Close() error {
	r.closeCount++
	return nil
}

type errorReadCloser struct {
	err error
}

func (r *errorReadCloser) Read([]byte) (int, error) {
	return 0, r.err
}

func (r *errorReadCloser) Close() error {
	return nil
}
