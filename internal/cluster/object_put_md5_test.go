package cluster

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// md5("hello") = 5d41402abc4b2a76b9719d911017c592
const helloMD5Hex = "5d41402abc4b2a76b9719d911017c592"

// readerOnly hides bytes.Reader's ReaderAt/Len/Size so a PUT skips the
// in-memory fast paths and takes the spool write path.
type readerOnly struct{ r io.Reader }

func (ro readerOnly) Read(p []byte) (int, error) { return ro.r.Read(p) }

// TestPutObject_FastPath_ContentMD5Mismatch: a small in-memory PUT (bytes.Reader
// → single-local fast path) with a wrong Content-MD5 must be rejected as
// BadDigest (storage.ErrContentMD5Mismatch) before commit.
func TestPutObject_FastPath_ContentMD5Mismatch(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "bucket"))

	_, err := b.PutObjectWithRequest(ctx, storage.PutObjectRequest{
		Bucket:        "bucket",
		Key:           "fast",
		Body:          bytes.NewReader([]byte("hello")),
		ContentMD5Hex: "deadbeefdeadbeefdeadbeefdeadbeef", // wrong
	})
	require.ErrorIs(t, err, storage.ErrContentMD5Mismatch)
}

// TestPutObject_SpoolPath_ContentMD5Mismatch: a PUT whose body is not a
// sized-reader-at (forces the spool path) with a wrong Content-MD5 must be
// rejected as BadDigest at the spool site (sp.ETag) before any shard write.
func TestPutObject_SpoolPath_ContentMD5Mismatch(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "bucket"))

	_, err := b.PutObjectWithRequest(ctx, storage.PutObjectRequest{
		Bucket:        "bucket",
		Key:           "spool",
		Body:          readerOnly{r: bytes.NewReader([]byte("hello"))},
		ContentMD5Hex: "deadbeefdeadbeefdeadbeefdeadbeef", // wrong
	})
	require.ErrorIs(t, err, storage.ErrContentMD5Mismatch)
}

// TestPutObject_ContentMD5Match: a correct Content-MD5 succeeds and the stored
// ETag equals the body md5 (both fast and spool paths).
func TestPutObject_ContentMD5Match(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "bucket"))

	for _, tc := range []struct {
		name string
		body io.Reader
		key  string
	}{
		{"fast", bytes.NewReader([]byte("hello")), "ok-fast"},
		{"spool", readerOnly{r: bytes.NewReader([]byte("hello"))}, "ok-spool"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			obj, err := b.PutObjectWithRequest(ctx, storage.PutObjectRequest{
				Bucket:        "bucket",
				Key:           tc.key,
				Body:          tc.body,
				ContentMD5Hex: helloMD5Hex,
			})
			require.NoError(t, err)
			require.Equal(t, helloMD5Hex, obj.ETag)
		})
	}
}
