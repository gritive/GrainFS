package nfs4server

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
)

type preferPutObjectBackend struct {
	storage.Backend
	writeAtCalled  bool
	readAtCalled   bool
	truncateCalled bool
	preferReadAt   bool
}

func (b *preferPutObjectBackend) PreferWriteAt(string) bool { return false }
func (b *preferPutObjectBackend) PreferReadAt(string) bool  { return b.preferReadAt }

func (b *preferPutObjectBackend) WriteAt(context.Context, string, string, uint64, []byte) (*storage.Object, error) {
	b.writeAtCalled = true
	return nil, errors.New("unexpected WriteAt")
}

func (b *preferPutObjectBackend) ReadAt(ctx context.Context, bucket, key string, off int64, p []byte) (int, error) {
	b.readAtCalled = true
	if !b.preferReadAt {
		return 0, errors.New("unexpected ReadAt")
	}
	return b.Backend.(storage.PartialIO).ReadAt(ctx, bucket, key, off, p)
}

func (b *preferPutObjectBackend) Truncate(context.Context, string, string, int64) error {
	b.truncateCalled = true
	return errors.New("unexpected Truncate")
}

func TestOpWriteHonorsPreferWriteAtFalse(t *testing.T) {
	local, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, local.Close()) })
	require.NoError(t, local.CreateBucket(context.Background(), "user-bucket"))

	backend := &preferPutObjectBackend{Backend: local}
	d := getDispatcherWithClient(backend, NewStateManager(), nil, "", nil)
	t.Cleanup(func() { putDispatcher(d) })
	d.currentPath = "/user-bucket/file.bin"

	w := &XDRWriter{}
	w.buf.Write(make([]byte, 16)) // stateid
	w.WriteUint64(0)
	w.WriteUint32(2)
	w.WriteOpaque([]byte("payload"))

	res := d.opWrite(w.Bytes())
	require.Equal(t, NFS4_OK, res.Status)
	require.False(t, backend.writeAtCalled)

	rc, _, err := local.GetObject(context.Background(), "user-bucket", "file.bin")
	require.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, "payload", string(got))
}

func TestOpWriteFallbackStreamsPartialOverwrite(t *testing.T) {
	local, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, local.Close()) })
	require.NoError(t, local.CreateBucket(context.Background(), "user-bucket"))
	_, err = local.PutObject(context.Background(), "user-bucket", "file.bin", strings.NewReader("hello"), "application/octet-stream")
	require.NoError(t, err)

	backend := &preferPutObjectBackend{Backend: local}
	d := getDispatcherWithClient(backend, NewStateManager(), nil, "", nil)
	t.Cleanup(func() { putDispatcher(d) })
	d.currentPath = "/user-bucket/file.bin"

	w := &XDRWriter{}
	w.buf.Write(make([]byte, 16)) // stateid
	w.WriteUint64(2)
	w.WriteUint32(2)
	w.WriteOpaque([]byte("YY"))

	res := d.opWrite(w.Bytes())
	require.Equal(t, NFS4_OK, res.Status)
	require.False(t, backend.writeAtCalled)

	rc, _, err := local.GetObject(context.Background(), "user-bucket", "file.bin")
	require.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, "heYYo", string(got))
}

func TestOpWriteFallbackUsesReadAtWhenPreferred(t *testing.T) {
	local, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, local.Close()) })
	require.NoError(t, local.CreateBucket(context.Background(), "user-bucket"))
	_, err = local.PutObject(context.Background(), "user-bucket", "file.bin", strings.NewReader("hello"), "application/octet-stream")
	require.NoError(t, err)

	backend := &preferPutObjectBackend{Backend: local, preferReadAt: true}
	d := getDispatcherWithClient(backend, NewStateManager(), nil, "", nil)
	t.Cleanup(func() { putDispatcher(d) })
	d.currentPath = "/user-bucket/file.bin"

	w := &XDRWriter{}
	w.buf.Write(make([]byte, 16)) // stateid
	w.WriteUint64(2)
	w.WriteUint32(2)
	w.WriteOpaque([]byte("YY"))

	res := d.opWrite(w.Bytes())
	require.Equal(t, NFS4_OK, res.Status)
	require.True(t, backend.readAtCalled)
	require.False(t, backend.writeAtCalled)

	rc, _, err := local.GetObject(context.Background(), "user-bucket", "file.bin")
	require.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, "heYYo", string(got))
}

func TestOpWriteFallbackStreamsSparseGap(t *testing.T) {
	local, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, local.Close()) })
	require.NoError(t, local.CreateBucket(context.Background(), "user-bucket"))

	backend := &preferPutObjectBackend{Backend: local}
	d := getDispatcherWithClient(backend, NewStateManager(), nil, "", nil)
	t.Cleanup(func() { putDispatcher(d) })
	d.currentPath = "/user-bucket/file.bin"

	w := &XDRWriter{}
	w.buf.Write(make([]byte, 16)) // stateid
	w.WriteUint64(3)
	w.WriteUint32(2)
	w.WriteOpaque([]byte("x"))

	res := d.opWrite(w.Bytes())
	require.Equal(t, NFS4_OK, res.Status)

	rc, _, err := local.GetObject(context.Background(), "user-bucket", "file.bin")
	require.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, []byte{0, 0, 0, 'x'}, got)
}

func TestOpWriteFallbackRejectsOffsetPastInt64(t *testing.T) {
	local, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, local.Close()) })
	require.NoError(t, local.CreateBucket(context.Background(), "user-bucket"))

	backend := &preferPutObjectBackend{Backend: local}
	d := getDispatcherWithClient(backend, NewStateManager(), nil, "", nil)
	t.Cleanup(func() { putDispatcher(d) })
	d.currentPath = "/user-bucket/file.bin"

	w := &XDRWriter{}
	w.buf.Write(make([]byte, 16)) // stateid
	w.WriteUint64(maxInt64Uint + 1)
	w.WriteUint32(2)
	w.WriteOpaque([]byte("x"))

	res := d.opWrite(w.Bytes())
	require.Equal(t, NFS4ERR_FBIG, res.Status)
}

func TestOpWriteFallbackRejectsHugeSparseGap(t *testing.T) {
	local, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, local.Close()) })
	require.NoError(t, local.CreateBucket(context.Background(), "user-bucket"))

	backend := &preferPutObjectBackend{Backend: local}
	d := getDispatcherWithClient(backend, NewStateManager(), nil, "", nil)
	t.Cleanup(func() { putDispatcher(d) })
	d.currentPath = "/user-bucket/file.bin"

	w := &XDRWriter{}
	w.buf.Write(make([]byte, 16)) // stateid
	w.WriteUint64(nfsMaxFallbackSparseSize + 1)
	w.WriteUint32(2)
	w.WriteOpaque([]byte("x"))

	res := d.opWrite(w.Bytes())
	require.Equal(t, NFS4ERR_FBIG, res.Status)
	_, _, err = local.GetObject(context.Background(), "user-bucket", "file.bin")
	require.ErrorIs(t, err, storage.ErrObjectNotFound)
}

func TestSetAttrSizeHonorsPreferWriteAtFalse(t *testing.T) {
	local, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, local.Close()) })
	require.NoError(t, local.CreateBucket(context.Background(), "user-bucket"))
	_, err = local.PutObject(context.Background(), "user-bucket", "file.bin", strings.NewReader("payload"), "application/octet-stream")
	require.NoError(t, err)

	backend := &preferPutObjectBackend{Backend: local}
	d := getDispatcherWithClient(backend, NewStateManager(), nil, "", nil)
	t.Cleanup(func() { putDispatcher(d) })
	d.currentPath = "/user-bucket/file.bin"

	attrVals := &XDRWriter{}
	attrVals.WriteUint64(3)
	w := &XDRWriter{}
	w.buf.Write(make([]byte, 16)) // stateid
	w.WriteUint32(1 << fattr4Size)
	w.WriteUint32(0)
	w.WriteOpaque(attrVals.Bytes())

	res := d.opSetAttr(w.Bytes())
	require.Equal(t, NFS4_OK, res.Status)
	require.False(t, backend.truncateCalled)

	rc, _, err := local.GetObject(context.Background(), "user-bucket", "file.bin")
	require.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, "pay", string(got))
}
