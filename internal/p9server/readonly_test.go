package p9server

import (
	"context"
	"strings"
	"syscall"
	"testing"

	"github.com/hugelgupf/p9/p9"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/nfsexport"
)

// stubExportStore is a minimal exportGetter for tests: returns ReadOnly=true
// for any bucket in the ro set.
type stubExportStore struct {
	ro map[string]bool
}

func (s *stubExportStore) Get(bucket string) (nfsexport.Config, bool) {
	ro, ok := s.ro[bucket]
	if !ok {
		return nfsexport.Config{}, false
	}
	return nfsexport.Config{ReadOnly: ro}, true
}

func newROExportStore(bucket string) *stubExportStore {
	return &stubExportStore{ro: map[string]bool{bucket: true}}
}

func newRWExportStore(bucket string) *stubExportStore {
	return &stubExportStore{ro: map[string]bool{bucket: false}}
}

// --- helper ---

func setupROBucket(t *testing.T, bucket string) (*bucketFile, *stubExportStore) {
	t.Helper()
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, bucket))
	exp := newROExportStore(bucket)
	bf := &bucketFile{
		backend:     backend,
		locks:       newObjectLocks(),
		bucket:      bucket,
		exportStore: exp,
	}
	return bf, exp
}

// --- WriteAt: ro export → EROFS ---

func TestP9_Write_ReadOnlyExport_EROFS(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	obj, err := backend.PutObject(ctx, "bkt", "file.txt", strings.NewReader("hello"), "text/plain")
	require.NoError(t, err)

	of := &objectFile{
		backend:     backend,
		locks:       newObjectLocks(),
		bucket:      "bkt",
		key:         "file.txt",
		meta:        obj,
		exportStore: newROExportStore("bkt"),
	}
	_, err = of.WriteAt([]byte("X"), 0)
	require.ErrorIs(t, err, syscall.EROFS)
}

// --- Create: ro export → EROFS ---

func TestP9_Create_ReadOnlyExport_EROFS(t *testing.T) {
	bf, _ := setupROBucket(t, "bkt")
	_, _, _, err := bf.Create("newfile.txt", p9.WriteOnly, 0644, 0, 0)
	require.ErrorIs(t, err, syscall.EROFS)
}

// --- Remove (UnlinkAt): ro export → EROFS ---

func TestP9_Remove_ReadOnlyExport_EROFS(t *testing.T) {
	bf, _ := setupROBucket(t, "bkt")
	ctx := context.Background()
	_, err := bf.backend.PutObject(ctx, "bkt", "file.txt", strings.NewReader("x"), "text/plain")
	require.NoError(t, err)
	err = bf.UnlinkAt("file.txt", 0)
	require.ErrorIs(t, err, syscall.EROFS)
}

// --- Mkdir: ro export → EROFS ---

func TestP9_Mkdir_ReadOnlyExport_EROFS(t *testing.T) {
	bf, _ := setupROBucket(t, "bkt")
	_, err := bf.Mkdir("newdir", 0755, 0, 0)
	require.ErrorIs(t, err, syscall.EROFS)
}

// --- RenameAt: ro export → EROFS ---

func TestP9_RenameAt_ReadOnlyExport_EROFS(t *testing.T) {
	bf, _ := setupROBucket(t, "bkt")
	ctx := context.Background()
	_, err := bf.backend.PutObject(ctx, "bkt", "old.txt", strings.NewReader("x"), "text/plain")
	require.NoError(t, err)
	err = bf.RenameAt("old.txt", bf, "new.txt")
	require.ErrorIs(t, err, syscall.EROFS)
}

// --- SetAttr size change: ro export → EROFS ---

func TestP9_Twstat_Size_ReadOnlyExport_EROFS(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	obj, err := backend.PutObject(ctx, "bkt", "file.txt", strings.NewReader("hello"), "text/plain")
	require.NoError(t, err)

	of := &objectFile{
		backend:     backend,
		locks:       newObjectLocks(),
		bucket:      "bkt",
		key:         "file.txt",
		meta:        obj,
		exportStore: newROExportStore("bkt"),
	}
	err = of.SetAttr(p9.SetAttrMask{Size: true}, p9.SetAttr{Size: 3})
	require.ErrorIs(t, err, syscall.EROFS)
}

// --- SetAttr mtime change: ro export → EROFS ---

func TestP9_Twstat_MTime_ReadOnlyExport_EROFS(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	obj, err := backend.PutObject(ctx, "bkt", "file.txt", strings.NewReader("hello"), "text/plain")
	require.NoError(t, err)

	of := &objectFile{
		backend:     backend,
		locks:       newObjectLocks(),
		bucket:      "bkt",
		key:         "file.txt",
		meta:        obj,
		exportStore: newROExportStore("bkt"),
	}
	err = of.SetAttr(p9.SetAttrMask{MTime: true}, p9.SetAttr{})
	require.ErrorIs(t, err, syscall.EROFS)
}

// --- SetAttr mode change: always EPERM (D#8 spec), even on ro export ---

func TestP9_Twstat_Mode_AlwaysPerm(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	obj, err := backend.PutObject(ctx, "bkt", "file.txt", strings.NewReader("hello"), "text/plain")
	require.NoError(t, err)

	// Test with RO export — mode change must return EPERM (not EROFS).
	of := &objectFile{
		backend:     backend,
		locks:       newObjectLocks(),
		bucket:      "bkt",
		key:         "file.txt",
		meta:        obj,
		exportStore: newROExportStore("bkt"),
	}
	err = of.SetAttr(p9.SetAttrMask{Permissions: true}, p9.SetAttr{Permissions: 0600})
	require.ErrorIs(t, err, syscall.EPERM)
}

// --- Read succeeds on ro export ---

func TestP9_Read_ReadOnlyExport_OK(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	obj, err := backend.PutObject(ctx, "bkt", "file.txt", strings.NewReader("hello"), "text/plain")
	require.NoError(t, err)

	of := &objectFile{
		backend:     backend,
		locks:       newObjectLocks(),
		bucket:      "bkt",
		key:         "file.txt",
		meta:        obj,
		exportStore: newROExportStore("bkt"),
	}
	buf := make([]byte, 5)
	n, err := of.ReadAt(buf, 0)
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, "hello", string(buf))
}

// --- SetAttr no-op (no flags set): succeeds even on ro export ---

func TestP9_Twstat_NoOp_ReadOnlyExport_OK(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	obj, err := backend.PutObject(ctx, "bkt", "file.txt", strings.NewReader("hello"), "text/plain")
	require.NoError(t, err)

	of := &objectFile{
		backend:     backend,
		locks:       newObjectLocks(),
		bucket:      "bkt",
		key:         "file.txt",
		meta:        obj,
		exportStore: newROExportStore("bkt"),
	}
	// No fields set → no-op → should succeed even on RO export.
	err = of.SetAttr(p9.SetAttrMask{}, p9.SetAttr{})
	require.NoError(t, err)
}

// --- Mutations succeed on rw export ---

func TestP9_Write_RWExport_OK(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	obj, err := backend.PutObject(ctx, "bkt", "file.txt", strings.NewReader("hello"), "text/plain")
	require.NoError(t, err)

	of := &objectFile{
		backend:     backend,
		locks:       newObjectLocks(),
		bucket:      "bkt",
		key:         "file.txt",
		meta:        obj,
		exportStore: newRWExportStore("bkt"),
	}
	_, err = of.WriteAt([]byte("X"), 0)
	require.NoError(t, err)
}

// --- Mutations succeed when no export store is wired (nil) ---

func TestP9_Write_NoExportStore_OK(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	obj, err := backend.PutObject(ctx, "bkt", "file.txt", strings.NewReader("hello"), "text/plain")
	require.NoError(t, err)

	of := &objectFile{
		backend:     backend,
		locks:       newObjectLocks(),
		bucket:      "bkt",
		key:         "file.txt",
		meta:        obj,
		exportStore: nil, // no gate
	}
	_, err = of.WriteAt([]byte("X"), 0)
	require.NoError(t, err)
}
