package p9server

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestP9Flush_PreservesExistingContentType verifies that writing dirty data
// (via WriteAt + Close/FSync) to an existing object does not clobber its
// Content-Type.
func TestP9Flush_PreservesExistingContentType(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	obj, err := backend.PutObject(ctx, "bkt", "photo.jpg",
		strings.NewReader("OLDBYTES"), "image/png")
	require.NoError(t, err)

	locks := newObjectLocks()
	// preferPutObjectBackend forces fallback path (no WriteAt), same as in server_test.go.
	ppoBackend := &preferPutObjectBackend{Backend: backend}
	of := &objectFile{backend: ppoBackend, locks: locks, bucket: "bkt", key: "photo.jpg", meta: obj}

	_, err = of.WriteAt([]byte("NEWBYTES"), 0)
	require.NoError(t, err)
	require.NoError(t, of.Close())

	head, err := backend.HeadObject(ctx, "bkt", "photo.jpg")
	require.NoError(t, err)
	require.Equal(t, "image/png", head.ContentType, "9P WriteAt+Close must not clobber existing Content-Type")
}

// TestP9Flush_NewFile_DefaultsToOctetStream verifies that writing a new object
// via 9P defaults the Content-Type to application/octet-stream.
func TestP9Flush_NewFile_DefaultsToOctetStream(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))

	locks := newObjectLocks()
	of := &objectFile{
		backend: backend,
		locks:   locks,
		bucket:  "bkt",
		key:     "new.bin",
		meta:    nil,
	}
	_, err := of.WriteAt([]byte("data"), 0)
	require.NoError(t, err)
	require.NoError(t, of.Close())

	head, err := backend.HeadObject(ctx, "bkt", "new.bin")
	require.NoError(t, err)
	require.Equal(t, "application/octet-stream", head.ContentType)
}

// TestP9Resize_PreservesExistingContentType verifies that a SetAttr size
// truncation via 9P does not clobber the existing Content-Type.
func TestP9Resize_PreservesExistingContentType(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	obj, err := backend.PutObject(ctx, "bkt", "doc.pdf",
		strings.NewReader("PDFDATA"), "application/pdf")
	require.NoError(t, err)

	locks := newObjectLocks()
	// preferPutObjectBackend forces fallback truncate (read+PutObject), not Truncate().
	ppoBackend := &preferPutObjectBackend{Backend: backend}
	of := &objectFile{backend: ppoBackend, locks: locks, bucket: "bkt", key: "doc.pdf", meta: obj}

	require.NoError(t, of.resize(ctx, 3))

	head, err := backend.HeadObject(ctx, "bkt", "doc.pdf")
	require.NoError(t, err)
	require.Equal(t, "application/pdf", head.ContentType, "resize must not clobber existing Content-Type")
	require.Equal(t, int64(3), head.Size)
}
