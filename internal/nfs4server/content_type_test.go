package nfs4server

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// TestWrite_PreservesExistingContentType verifies that a full-overwrite NFS
// WRITE (offset=0, end>=existingSize) on an existing object does not clobber
// its Content-Type.
func TestWrite_PreservesExistingContentType(t *testing.T) {
	local, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, local.Close()) })
	require.NoError(t, local.CreateBucket(context.Background(), "bkt"))
	_, err = local.PutObject(context.Background(), "bkt", "photo.jpg",
		strings.NewReader("OLDBYTES"), "image/png")
	require.NoError(t, err)

	// Use preferPutObjectBackend so we exercise the RMW/PutObject path, not WriteAt.
	backend := &preferPutObjectBackend{Backend: local}
	d := getDispatcherWithClient(backend, NewStateManager(), nil, "", nil)
	t.Cleanup(func() { putDispatcher(d) })
	d.currentPath = "/bkt/photo.jpg"

	// offset=0, full replace
	w := &XDRWriter{}
	w.buf.Write(make([]byte, 16)) // stateid
	w.WriteUint64(0)
	w.WriteUint32(2)
	w.WriteOpaque([]byte("NEWBYTES"))
	res := d.opWrite(w.Bytes())
	require.Equal(t, NFS4_OK, res.Status)

	obj, err := local.HeadObject(context.Background(), "bkt", "photo.jpg")
	require.NoError(t, err)
	require.Equal(t, "image/png", obj.ContentType, "full-overwrite WRITE must not clobber existing Content-Type")
}

// TestWrite_NewFile_DefaultsToOctetStream verifies that writing to a new
// object defaults the Content-Type to application/octet-stream.
func TestWrite_NewFile_DefaultsToOctetStream(t *testing.T) {
	local, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, local.Close()) })
	require.NoError(t, local.CreateBucket(context.Background(), "bkt"))

	backend := &preferPutObjectBackend{Backend: local}
	d := getDispatcherWithClient(backend, NewStateManager(), nil, "", nil)
	t.Cleanup(func() { putDispatcher(d) })
	d.currentPath = "/bkt/new.bin"

	w := &XDRWriter{}
	w.buf.Write(make([]byte, 16)) // stateid
	w.WriteUint64(0)
	w.WriteUint32(2)
	w.WriteOpaque([]byte("data"))
	res := d.opWrite(w.Bytes())
	require.Equal(t, NFS4_OK, res.Status)

	obj, err := local.HeadObject(context.Background(), "bkt", "new.bin")
	require.NoError(t, err)
	require.Equal(t, "application/octet-stream", obj.ContentType)
}

// TestRMW_PreservesExistingContentType verifies that a partial-overwrite WRITE
// (offset>0) on an existing object does not clobber its Content-Type.
func TestRMW_PreservesExistingContentType(t *testing.T) {
	local, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, local.Close()) })
	require.NoError(t, local.CreateBucket(context.Background(), "bkt"))
	_, err = local.PutObject(context.Background(), "bkt", "doc.pdf",
		strings.NewReader("ABCDEF"), "application/pdf")
	require.NoError(t, err)

	backend := &preferPutObjectBackend{Backend: local}
	d := getDispatcherWithClient(backend, NewStateManager(), nil, "", nil)
	t.Cleanup(func() { putDispatcher(d) })
	d.currentPath = "/bkt/doc.pdf"

	// offset=2, partial write triggers RMW path
	w := &XDRWriter{}
	w.buf.Write(make([]byte, 16)) // stateid
	w.WriteUint64(2)
	w.WriteUint32(2)
	w.WriteOpaque([]byte("XY"))
	res := d.opWrite(w.Bytes())
	require.Equal(t, NFS4_OK, res.Status)

	obj, err := local.HeadObject(context.Background(), "bkt", "doc.pdf")
	require.NoError(t, err)
	require.Equal(t, "application/pdf", obj.ContentType, "RMW WRITE must not clobber existing Content-Type")

	rc, _, err := local.GetObject(context.Background(), "bkt", "doc.pdf")
	require.NoError(t, err)
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	require.Equal(t, "ABXYEF", string(got))
}

// TestSetAttr_Truncate_PreservesExistingContentType verifies that a SETATTR
// size truncation on an existing object preserves its Content-Type.
func TestSetAttr_Truncate_PreservesExistingContentType(t *testing.T) {
	local, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, local.Close()) })
	require.NoError(t, local.CreateBucket(context.Background(), "bkt"))
	_, err = local.PutObject(context.Background(), "bkt", "video.mp4",
		strings.NewReader("BIGDATA"), "video/mp4")
	require.NoError(t, err)

	// preferPutObjectBackend forces the fallback (read+PutObject) truncate path.
	backend := &preferPutObjectBackend{Backend: local}
	d := getDispatcherWithClient(backend, NewStateManager(), nil, "", nil)
	t.Cleanup(func() { putDispatcher(d) })
	d.currentPath = "/bkt/video.mp4"

	// Build SETATTR with FATTR4_SIZE=3
	attrVals := &XDRWriter{}
	attrVals.WriteUint64(3) // new size
	attrValsBytes := attrVals.Bytes()

	sa := &XDRWriter{}
	sa.buf.Write(make([]byte, 16)) // stateid
	sa.WriteUint32(1 << 4)         // bm0: FATTR4_SIZE
	sa.WriteUint32(0)              // bm1
	sa.WriteOpaque(attrValsBytes)

	res := d.opSetAttr(sa.Bytes())
	require.Equal(t, NFS4_OK, res.Status)

	obj, err := local.HeadObject(context.Background(), "bkt", "video.mp4")
	require.NoError(t, err)
	require.Equal(t, "video/mp4", obj.ContentType, "SETATTR truncate must not clobber existing Content-Type")
	require.Equal(t, int64(3), obj.Size)
}
