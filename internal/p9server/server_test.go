package p9server

import (
	"context"
	"errors"
	"io"
	"math"
	"net"
	"strings"
	"syscall"
	"testing"

	"bytes"
	"fmt"
	"time"

	"github.com/hugelgupf/p9/p9"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

func newTestBackend(t *testing.T) storage.Backend {
	t.Helper()
	b, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	return b
}

// Task 2: rootFile tests

func TestRootFile_Walk_EmptyReturnsClone(t *testing.T) {
	root := &rootFile{backend: newTestBackend(t)}
	qids, file, err := root.Walk(nil)
	require.NoError(t, err)
	require.Empty(t, qids)
	require.NotNil(t, file)
	_, ok := file.(*rootFile)
	require.True(t, ok)
}

func TestRootFile_Walk_UnknownBucket_ENOENT(t *testing.T) {
	root := &rootFile{backend: newTestBackend(t)}
	_, _, err := root.Walk([]string{"no-such-bucket"})
	require.ErrorIs(t, err, syscall.ENOENT)
}

func TestRootFile_Walk_ExistingBucket_ReturnsBucketFile(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "my-bucket"))

	root := &rootFile{backend: backend}
	qids, file, err := root.Walk([]string{"my-bucket"})
	require.NoError(t, err)
	require.Len(t, qids, 1)
	_, ok := file.(*bucketFile)
	require.True(t, ok)
}

func TestRootFile_Readdir_ListsBuckets(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "alpha"))
	require.NoError(t, backend.CreateBucket(ctx, "beta"))

	root := &rootFile{backend: backend}
	dirents, err := root.Readdir(0, 100)
	require.NoError(t, err)
	names := make([]string, len(dirents))
	for i, d := range dirents {
		names[i] = d.Name
	}
	require.Contains(t, names, "alpha")
	require.Contains(t, names, "beta")
}

// Task 3: bucketFile tests

func TestBucketFile_Walk_UnknownKey_ENOENT(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))

	bf := &bucketFile{backend: backend, bucket: "bkt"}
	_, _, err := bf.Walk([]string{"no-such-key.txt"})
	require.ErrorIs(t, err, syscall.ENOENT)
}

func TestBucketFile_Walk_ExistingKey_ReturnsObjectFile(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	_, err := backend.PutObject(ctx, "bkt", "hello.txt", strings.NewReader("hi"), "text/plain")
	require.NoError(t, err)

	bf := &bucketFile{backend: backend, bucket: "bkt"}
	qids, file, err := bf.Walk([]string{"hello.txt"})
	require.NoError(t, err)
	require.Len(t, qids, 1)
	_, ok := file.(*objectFile)
	require.True(t, ok)
}

func TestBucketFile_Walk_NestedKeyReturnsObjectFile(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	_, err := backend.PutObject(ctx, "bkt", "obj/000001.bin", strings.NewReader("hi"), "application/octet-stream")
	require.NoError(t, err)

	bf := &bucketFile{backend: backend, bucket: "bkt"}
	qids, file, err := bf.Walk([]string{"obj", "000001.bin"})
	require.NoError(t, err)
	require.Len(t, qids, 2)
	_, ok := file.(*objectFile)
	require.True(t, ok)
}

func TestBucketFile_Readdir_ListsObjects(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	_, err := backend.PutObject(ctx, "bkt", "a.txt", strings.NewReader("a"), "text/plain")
	require.NoError(t, err)
	_, err = backend.PutObject(ctx, "bkt", "b.txt", strings.NewReader("b"), "text/plain")
	require.NoError(t, err)

	bf := &bucketFile{backend: backend, bucket: "bkt"}
	dirents, err := bf.Readdir(0, 100)
	require.NoError(t, err)
	names := make([]string, len(dirents))
	for i, d := range dirents {
		names[i] = d.Name
	}
	require.Contains(t, names, "a.txt")
	require.Contains(t, names, "b.txt")
}

func TestBucketFile_Readdir_ListsSyntheticDirectories(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	_, err := backend.PutObject(ctx, "bkt", "obj/000001.bin", strings.NewReader("x"), "application/octet-stream")
	require.NoError(t, err)

	bf := &bucketFile{backend: backend, bucket: "bkt"}
	dirents, err := bf.Readdir(0, 100)
	require.NoError(t, err)
	require.Len(t, dirents, 1)
	require.Equal(t, "obj", dirents[0].Name)
	require.Equal(t, p9.TypeDir, dirents[0].Type)

	qids, file, err := bf.Walk([]string{"obj"})
	require.NoError(t, err)
	require.Len(t, qids, 1)
	child, ok := file.(*bucketFile)
	require.True(t, ok)

	dirents, err = child.Readdir(0, 100)
	require.NoError(t, err)
	require.Len(t, dirents, 1)
	require.Equal(t, "000001.bin", dirents[0].Name)
	require.Equal(t, p9.TypeRegular, dirents[0].Type)
}

func TestBucketFile_Readdir_StopsAfterCount(t *testing.T) {
	backend := &countingWalkBackend{objects: make([]*storage.Object, 1000)}
	for i := range backend.objects {
		backend.objects[i] = &storage.Object{Key: fmt.Sprintf("obj-%04d", i)}
	}

	bf := &bucketFile{backend: backend, bucket: "bkt"}
	dirents, err := bf.Readdir(0, 10)
	require.NoError(t, err)
	require.Len(t, dirents, 10)
	require.Equal(t, 10, backend.walked)
}

func TestBucketFile_Readdir_UsesKeyWalker(t *testing.T) {
	backend := &keyWalkingBackend{
		keys: []string{"a.txt", "b.txt"},
	}

	bf := &bucketFile{backend: backend, bucket: "bkt"}
	dirents, err := bf.Readdir(0, 10)
	require.NoError(t, err)
	require.Len(t, dirents, 2)
	require.Equal(t, 2, backend.keysWalked)
	require.Equal(t, 0, backend.objectsWalked)
}

func TestP9SidecarKey_ProtectedNamespace(t *testing.T) {
	require.True(t, isP9ReservedKey("__meta/file.txt"))
	require.True(t, isP9ReservedName("__meta"))
	require.False(t, isP9ReservedKey("user/__meta/file.txt"))
	require.False(t, isP9ReservedName("meta"))
}

func TestBucketFile_Readdir_HidesSidecarObjects(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	_, err := backend.PutObject(ctx, "bkt", "visible.txt", strings.NewReader("x"), "text/plain")
	require.NoError(t, err)
	_, err = backend.PutObject(ctx, "bkt", "__meta/visible.txt", strings.NewReader(`{"mode":384}`), "application/json")
	require.NoError(t, err)

	bf := &bucketFile{backend: backend, locks: newObjectLocks(), bucket: "bkt"}
	dirents, err := bf.Readdir(0, 100)
	require.NoError(t, err)
	require.Len(t, dirents, 1)
	require.Equal(t, "visible.txt", dirents[0].Name)

	_, _, err = bf.Walk([]string{"__meta"})
	require.ErrorIs(t, err, syscall.ENOENT)
}

func TestBucketFile_Symlink_CreatesSymlinkAndReturnsWalkableLink(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	_, err := backend.PutObject(ctx, "bkt", "target.txt", strings.NewReader("target"), "text/plain")
	require.NoError(t, err)

	bf := &bucketFile{backend: backend, locks: newObjectLocks(), bucket: "bkt"}
	qid, err := bf.Symlink("target.txt", "link", 0, 0)
	require.NoError(t, err)
	require.Equal(t, p9.TypeSymlink, qid.Type)
	require.Equal(t, []byte("target.txt"), readObjectBytes(t, backend, "bkt", "link"))

	saved := loadP9FileMeta(ctx, backend, "bkt", "link")
	require.Equal(t, uint32(p9.ModeSymlink|0777), saved.Mode)
	require.Equal(t, "target.txt", saved.Target)

	_, file, err := bf.Walk([]string{"link"})
	require.NoError(t, err)
	of := file.(*objectFile)
	_, _, attr, err := of.GetAttr(p9.AttrMask{Mode: true, Size: true})
	require.NoError(t, err)
	require.Equal(t, p9.ModeSymlink|0777, attr.Mode)
	target, err := of.Readlink()
	require.NoError(t, err)
	require.Equal(t, "target.txt", target)
}

func TestBucketFile_Readdir_ListedAsSymlink(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	_, err := backend.PutObject(ctx, "bkt", "target.txt", strings.NewReader("target"), "text/plain")
	require.NoError(t, err)

	bf := &bucketFile{backend: backend, locks: newObjectLocks(), bucket: "bkt"}
	_, err = bf.Symlink("target.txt", "link", 0, 0)
	require.NoError(t, err)

	dirents, err := bf.Readdir(0, 100)
	require.NoError(t, err)
	seenSymlink := false
	for _, d := range dirents {
		if d.Name == "link" {
			seenSymlink = true
			require.Equal(t, p9.TypeSymlink, d.Type)
		}
	}
	require.True(t, seenSymlink)
}

func TestBucketFile_SymlinkedEntry_RemovedFromDirectory(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	_, err := backend.PutObject(ctx, "bkt", "gone.txt", strings.NewReader("bye"), "text/plain")
	require.NoError(t, err)
	require.NoError(t, saveP9FileMeta(ctx, backend, "bkt", "gone.txt", p9FileMeta{Mode: 0600}))
	bf := &bucketFile{backend: backend, locks: newObjectLocks(), bucket: "bkt"}
	require.NoError(t, bf.UnlinkAt("gone.txt", 0))
	_, err = backend.HeadObject(ctx, "bkt", "gone.txt")
	require.Error(t, err)
	_, err = backend.HeadObject(ctx, "bkt", p9MetaSidecarKey("gone.txt"))
	require.Error(t, err)
}

func TestBucketFile_Create_ExistingFailsAndPreservesBytes(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	_, err := backend.PutObject(ctx, "bkt", "same.txt", strings.NewReader("keep"), "text/plain")
	require.NoError(t, err)
	bf := &bucketFile{backend: backend, locks: newObjectLocks(), bucket: "bkt"}
	_, _, _, err = bf.Create("same.txt", p9.WriteOnly, 0644, 0, 0)
	require.ErrorIs(t, err, syscall.EEXIST)
	require.Equal(t, []byte("keep"), readObjectBytes(t, backend, "bkt", "same.txt"))
}

func TestBucketFile_Create_RejectsSidecarNamespace(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	bf := &bucketFile{backend: backend, locks: newObjectLocks(), bucket: "bkt"}
	_, _, _, err := bf.Create("__meta", p9.WriteOnly, 0644, 0, 0)
	require.ErrorIs(t, err, syscall.EPERM)
}

func TestBucketFile_Mkdir_CreatesEmptyDirectory(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	bf := &bucketFile{backend: backend, locks: newObjectLocks(), bucket: "bkt"}

	qid, err := bf.Mkdir("dir", 0700, 0, 0)
	require.NoError(t, err)
	require.Equal(t, p9.TypeDir, qid.Type)

	dirents, err := bf.Readdir(0, 100)
	require.NoError(t, err)
	require.Len(t, dirents, 1)
	require.Equal(t, "dir", dirents[0].Name)
	require.Equal(t, p9.TypeDir, dirents[0].Type)

	qids, file, err := bf.Walk([]string{"dir"})
	require.NoError(t, err)
	require.Len(t, qids, 1)
	child, ok := file.(*bucketFile)
	require.True(t, ok)
	dirents, err = child.Readdir(0, 100)
	require.NoError(t, err)
	require.Empty(t, dirents)
	_, _, attr, err := child.GetAttr(p9.AttrMask{Mode: true})
	require.NoError(t, err)
	require.Equal(t, p9.ModeDirectory|0700, attr.Mode)
}

func TestBucketFile_Mkdir_ExistingDirectoryFails(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	bf := &bucketFile{backend: backend, locks: newObjectLocks(), bucket: "bkt"}

	_, err := bf.Mkdir("dir", 0755, 0, 0)
	require.NoError(t, err)
	_, err = bf.Mkdir("dir", 0755, 0, 0)
	require.ErrorIs(t, err, syscall.EEXIST)
	_, _, _, err = bf.Create("dir", p9.WriteOnly, 0644, 0, 0)
	require.ErrorIs(t, err, syscall.EISDIR)
}

func TestBucketFile_Mkdir_ExistingFileFailsAndPreservesBytes(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	_, err := backend.PutObject(ctx, "bkt", "file.txt", strings.NewReader("keep"), "text/plain")
	require.NoError(t, err)
	bf := &bucketFile{backend: backend, locks: newObjectLocks(), bucket: "bkt"}

	_, err = bf.Mkdir("file.txt", 0755, 0, 0)
	require.ErrorIs(t, err, syscall.EEXIST)
	require.Equal(t, []byte("keep"), readObjectBytes(t, backend, "bkt", "file.txt"))
}

func TestBucketFile_Mkdir_RejectsSidecarNamespace(t *testing.T) {
	backend := newTestBackend(t)
	require.NoError(t, backend.CreateBucket(context.Background(), "bkt"))
	bf := &bucketFile{backend: backend, locks: newObjectLocks(), bucket: "bkt"}

	_, err := bf.Mkdir("__meta", 0755, 0, 0)
	require.ErrorIs(t, err, syscall.EPERM)
}

func TestBucketFile_ChildCreateSharesParentDirectoryLock(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	locks := newObjectLocks()
	child := &bucketFile{backend: backend, locks: locks, bucket: "bkt", prefix: "dir/"}

	unlock := locks.lock("bkt", "dir/")
	done := make(chan error, 1)
	go func() {
		_, _, _, err := child.Create("file.txt", p9.WriteOnly, 0644, 0, 0)
		done <- err
	}()

	select {
	case err := <-done:
		unlock()
		require.NoError(t, err)
		t.Fatal("child create completed while parent directory lock was held")
	case <-time.After(20 * time.Millisecond):
	}

	unlock()
	require.NoError(t, <-done)
}

func TestBucketFile_UnlinkAt_DeletesFileAndMetadata_WithMetadata(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	_, err := backend.PutObject(ctx, "bkt", "gone.txt", strings.NewReader("bye"), "text/plain")
	require.NoError(t, err)
	require.NoError(t, saveP9FileMeta(ctx, backend, "bkt", "gone.txt", p9FileMeta{Mode: 0600}))
	bf := &bucketFile{backend: backend, locks: newObjectLocks(), bucket: "bkt"}
	require.NoError(t, bf.UnlinkAt("gone.txt", 0))
	_, err = backend.HeadObject(ctx, "bkt", "gone.txt")
	require.Error(t, err)
	_, err = backend.HeadObject(ctx, "bkt", p9MetaSidecarKey("gone.txt"))
	require.Error(t, err)
}

func TestBucketFile_UnlinkAt_MissingReturnsENOENT(t *testing.T) {
	backend := newTestBackend(t)
	require.NoError(t, backend.CreateBucket(context.Background(), "bkt"))
	bf := &bucketFile{backend: backend, locks: newObjectLocks(), bucket: "bkt"}
	err := bf.UnlinkAt("missing.txt", 0)
	require.ErrorIs(t, err, syscall.ENOENT)
}

func TestBucketFile_UnlinkAt_RejectsReserved(t *testing.T) {
	backend := newTestBackend(t)
	require.NoError(t, backend.CreateBucket(context.Background(), "bkt"))
	bf := &bucketFile{backend: backend, locks: newObjectLocks(), bucket: "bkt"}
	err := bf.UnlinkAt("__meta", 0)
	require.ErrorIs(t, err, syscall.EPERM)
}

func TestBucketFile_UnlinkAt_RemovesEmptyDirectory(t *testing.T) {
	const atRemovedir = 0x200
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	bf := &bucketFile{backend: backend, locks: newObjectLocks(), bucket: "bkt"}

	_, err := bf.Mkdir("dir", 0755, 0, 0)
	require.NoError(t, err)
	require.NoError(t, bf.UnlinkAt("dir", atRemovedir))
	_, _, err = bf.Walk([]string{"dir"})
	require.ErrorIs(t, err, syscall.ENOENT)
	_, err = backend.HeadObject(ctx, "bkt", p9MetaSidecarKey("dir/"))
	require.Error(t, err)
}

func TestBucketFile_UnlinkAt_NonEmptyDirectoryFails(t *testing.T) {
	const atRemovedir = 0x200
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	_, err := backend.PutObject(ctx, "bkt", "dir/file.txt", strings.NewReader("x"), "text/plain")
	require.NoError(t, err)
	bf := &bucketFile{backend: backend, locks: newObjectLocks(), bucket: "bkt"}

	err = bf.UnlinkAt("dir", atRemovedir)
	require.ErrorIs(t, err, syscall.ENOTEMPTY)
}

func TestBucketFile_RenameAt_SameBucketFilePreservesMetadata(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	_, err := backend.PutObject(ctx, "bkt", "old.txt", strings.NewReader("data"), "text/plain")
	require.NoError(t, err)
	require.NoError(t, saveP9FileMeta(ctx, backend, "bkt", "old.txt", p9FileMeta{Mode: 0600, Mtime: 99}))
	bf := &bucketFile{backend: backend, locks: newObjectLocks(), bucket: "bkt"}
	require.NoError(t, bf.RenameAt("old.txt", bf, "new.txt"))
	require.Equal(t, []byte("data"), readObjectBytes(t, backend, "bkt", "new.txt"))
	_, err = backend.HeadObject(ctx, "bkt", "old.txt")
	require.Error(t, err)
	require.Equal(t, uint32(0600), loadP9FileMeta(ctx, backend, "bkt", "new.txt").Mode)
	_, err = backend.HeadObject(ctx, "bkt", p9MetaSidecarKey("old.txt"))
	require.Error(t, err)
}

func TestBucketFile_RenameAt_SamePathIsNoop(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	_, err := backend.PutObject(ctx, "bkt", "same.txt", strings.NewReader("data"), "text/plain")
	require.NoError(t, err)
	require.NoError(t, saveP9FileMeta(ctx, backend, "bkt", "same.txt", p9FileMeta{Mode: 0600}))

	bf := &bucketFile{backend: backend, locks: newObjectLocks(), bucket: "bkt"}
	require.NoError(t, bf.RenameAt("same.txt", bf, "same.txt"))
	require.Equal(t, []byte("data"), readObjectBytes(t, backend, "bkt", "same.txt"))
	require.Equal(t, uint32(0600), loadP9FileMeta(ctx, backend, "bkt", "same.txt").Mode)
}

func TestBucketFile_RenameAt_ExistingDirectoryFails(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	_, err := backend.PutObject(ctx, "bkt", "file.txt", strings.NewReader("data"), "text/plain")
	require.NoError(t, err)
	bf := &bucketFile{backend: backend, locks: newObjectLocks(), bucket: "bkt"}
	_, err = bf.Mkdir("dir", 0755, 0, 0)
	require.NoError(t, err)

	err = bf.RenameAt("file.txt", bf, "dir")
	require.ErrorIs(t, err, syscall.EISDIR)
	require.Equal(t, []byte("data"), readObjectBytes(t, backend, "bkt", "file.txt"))
	qids, file, err := bf.Walk([]string{"dir"})
	require.NoError(t, err)
	require.Len(t, qids, 1)
	_, ok := file.(*bucketFile)
	require.True(t, ok)
}

func TestBucketFile_RenameAt_CrossBucketEXDEVAndReservedRejected(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "src"))
	require.NoError(t, backend.CreateBucket(ctx, "dst"))
	locks := newObjectLocks()
	src := &bucketFile{backend: backend, locks: locks, bucket: "src"}
	dst := &bucketFile{backend: backend, locks: locks, bucket: "dst"}
	err := src.RenameAt("old.txt", dst, "new.txt")
	require.ErrorIs(t, err, syscall.EXDEV)
	err = src.RenameAt("__meta", src, "new.txt")
	require.ErrorIs(t, err, syscall.EPERM)
}

func Test9PVersioning_RenameFromDeleteMarkerReturnsENOENT(t *testing.T) {
	backend := &deleteMarkerHeadBackend{Backend: newTestBackend(t)}
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	_, err := backend.PutObject(ctx, "bkt", "old.txt", strings.NewReader("data"), "text/plain")
	require.NoError(t, err)
	backend.deleteMarkerKey = "old.txt"

	bf := &bucketFile{backend: backend, locks: newObjectLocks(), bucket: "bkt"}
	err = bf.RenameAt("old.txt", bf, "new.txt")
	require.ErrorIs(t, err, syscall.ENOENT)
}

func Test9PRecoveryWriteGate_MutationsFailAndPreserveBytes(t *testing.T) {
	base := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, base.CreateBucket(ctx, "bkt"))
	obj, err := base.PutObject(ctx, "bkt", "file.txt", strings.NewReader("keep"), "text/plain")
	require.NoError(t, err)
	_, err = base.PutObject(ctx, "bkt", "rename.txt", strings.NewReader("move"), "text/plain")
	require.NoError(t, err)

	gate := storage.NewRecoveryWriteGate(base, storage.ErrRecoveryWriteDisabled)
	locks := newObjectLocks()
	bf := &bucketFile{backend: gate, locks: locks, bucket: "bkt"}
	of := &objectFile{backend: gate, locks: locks, bucket: "bkt", key: "file.txt", meta: obj}

	_, _, _, err = bf.Create("new.txt", p9.WriteOnly, 0644, 0, 0)
	require.Error(t, err)
	_, err = of.WriteAt([]byte("X"), 0)
	require.Error(t, err)
	err = of.SetAttr(p9.SetAttrMask{Size: true}, p9.SetAttr{Size: 1})
	require.Error(t, err)
	err = bf.UnlinkAt("file.txt", 0)
	require.Error(t, err)
	err = bf.RenameAt("rename.txt", bf, "renamed.txt")
	require.Error(t, err)

	require.Equal(t, []byte("keep"), readObjectBytes(t, base, "bkt", "file.txt"))
	require.Equal(t, []byte("move"), readObjectBytes(t, base, "bkt", "rename.txt"))
}

func Test9PMetadata_NFSCacheStalenessDocumented(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	_, err := backend.PutObject(ctx, "bkt", "file.txt", strings.NewReader("data"), "text/plain")
	require.NoError(t, err)

	require.NoError(t, saveP9FileMeta(ctx, backend, "bkt", "file.txt", p9FileMeta{Mode: 0600}))
	require.Equal(t, uint32(0600), loadP9FileMeta(ctx, backend, "bkt", "file.txt").Mode)
	require.NoError(t, saveP9FileMeta(ctx, backend, "bkt", "file.txt", p9FileMeta{Mode: 0640}))
	require.Equal(t, uint32(0640), loadP9FileMeta(ctx, backend, "bkt", "file.txt").Mode)
	t.Log("9P metadata helper is uncached; NFS keeps a separate in-process sidecar cache and is not invalidated by this package.")
}

// Task 4: objectFile tests

func TestObjectFile_GetAttr_SizeAndMtime(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	obj, err := backend.PutObject(ctx, "bkt", "hello.txt", strings.NewReader("hello world"), "text/plain")
	require.NoError(t, err)

	of := &objectFile{backend: backend, bucket: "bkt", key: "hello.txt", meta: obj}
	_, valid, attr, err := of.GetAttr(p9.AttrMask{Size: true, MTime: true})
	require.NoError(t, err)
	require.True(t, valid.Size)
	require.True(t, valid.MTime)
	require.Equal(t, uint64(11), attr.Size)
	require.Equal(t, uint64(obj.LastModified), attr.MTimeSeconds)
}

func TestObjectFile_GetAttr_UsesSidecarModeAndMtimeWithFreshHead(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	obj, err := backend.PutObject(ctx, "bkt", "hello.txt", strings.NewReader("hello"), "text/plain")
	require.NoError(t, err)
	require.NoError(t, saveP9FileMeta(ctx, backend, "bkt", "hello.txt", p9FileMeta{
		Mode:  0600,
		Mtime: time.Unix(1704067200, 123).UnixNano(),
	}))

	of := &objectFile{backend: backend, bucket: "bkt", key: "hello.txt", meta: obj}
	_, err = backend.PutObject(ctx, "bkt", "hello.txt", strings.NewReader("hello world"), "text/plain")
	require.NoError(t, err)

	_, valid, attr, err := of.GetAttr(p9.AttrMask{Mode: true, Size: true, MTime: true})
	require.NoError(t, err)
	require.True(t, valid.Mode)
	require.True(t, valid.Size)
	require.True(t, valid.MTime)
	require.Equal(t, p9.ModeRegular|0600, attr.Mode)
	require.Equal(t, uint64(11), attr.Size)
	require.Equal(t, uint64(1704067200), attr.MTimeSeconds)
	require.Equal(t, uint64(123), attr.MTimeNanoSeconds)
}

func TestObjectFile_SetAttr_ModeMtimeAndServerTime(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	obj, err := backend.PutObject(ctx, "bkt", "hello.txt", strings.NewReader("hello"), "text/plain")
	require.NoError(t, err)
	of := &objectFile{backend: backend, locks: newObjectLocks(), bucket: "bkt", key: "hello.txt", meta: obj}

	err = of.SetAttr(p9.SetAttrMask{Permissions: true, MTime: true, MTimeNotSystemTime: true},
		p9.SetAttr{Permissions: 0600, MTimeSeconds: 1704067200, MTimeNanoSeconds: 55})
	require.NoError(t, err)
	meta := loadP9FileMeta(ctx, backend, "bkt", "hello.txt")
	require.Equal(t, uint32(0600), meta.Mode)
	require.Equal(t, time.Unix(1704067200, 55).UnixNano(), meta.Mtime)

	before := time.Now().Add(-time.Second).UnixNano()
	err = of.SetAttr(p9.SetAttrMask{MTime: true}, p9.SetAttr{})
	require.NoError(t, err)
	after := loadP9FileMeta(ctx, backend, "bkt", "hello.txt").Mtime
	require.GreaterOrEqual(t, after, before)
}

func TestObjectFile_SetAttr_SizeRejectsOverflow(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	obj, err := backend.PutObject(ctx, "bkt", "data.bin", strings.NewReader("abc"), "application/octet-stream")
	require.NoError(t, err)
	of := &objectFile{backend: backend, locks: newObjectLocks(), bucket: "bkt", key: "data.bin", meta: obj}
	err = of.SetAttr(p9.SetAttrMask{Size: true}, p9.SetAttr{Size: uint64(math.MaxInt64) + 1})
	require.ErrorIs(t, err, syscall.EFBIG)
}

func TestObjectFile_SetAttr_SizeRejectsLargeFallback(t *testing.T) {
	backend := &preferPutObjectBackend{Backend: newTestBackend(t)}
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	obj, err := backend.PutObject(ctx, "bkt", "data.bin", strings.NewReader("abc"), "application/octet-stream")
	require.NoError(t, err)
	of := &objectFile{backend: backend, locks: newObjectLocks(), bucket: "bkt", key: "data.bin", meta: obj}
	err = of.SetAttr(p9.SetAttrMask{Size: true}, p9.SetAttr{Size: maxFallbackObjectSize + 1})
	require.ErrorIs(t, err, syscall.EFBIG)
	require.False(t, backend.truncateCalled)
}

func TestObjectFile_SetAttr_TruncateAndExtend(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	obj, err := backend.PutObject(ctx, "bkt", "data.bin", strings.NewReader("abcdef"), "application/octet-stream")
	require.NoError(t, err)
	of := &objectFile{backend: backend, locks: newObjectLocks(), bucket: "bkt", key: "data.bin", meta: obj}
	require.NoError(t, of.SetAttr(p9.SetAttrMask{Size: true}, p9.SetAttr{Size: 3}))
	require.Equal(t, []byte("abc"), readObjectBytes(t, backend, "bkt", "data.bin"))
	require.NoError(t, of.SetAttr(p9.SetAttrMask{Size: true}, p9.SetAttr{Size: 5}))
	require.Equal(t, []byte{'a', 'b', 'c', 0, 0}, readObjectBytes(t, backend, "bkt", "data.bin"))
}

func TestObjectFile_Open_SymlinkReturnsEPERM(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	obj, err := backend.PutObject(ctx, "bkt", "link", strings.NewReader("hello"), "text/plain")
	require.NoError(t, err)
	require.NoError(t, saveP9FileMeta(ctx, backend, "bkt", "link", p9FileMeta{Mode: uint32(p9.ModeSymlink | 0777)}))
	of := &objectFile{backend: backend, locks: newObjectLocks(), bucket: "bkt", key: "link", meta: obj}
	_, _, err = of.Open(p9.ReadOnly)
	require.ErrorIs(t, err, syscall.EPERM)
}

func TestObjectFile_Readlink_ReturnsSymlinkTargetFromMeta(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	obj, err := backend.PutObject(ctx, "bkt", "link", strings.NewReader("../../target.txt"), "text/plain")
	require.NoError(t, err)
	require.NoError(t, saveP9FileMeta(ctx, backend, "bkt", "link", p9FileMeta{Mode: uint32(p9.ModeSymlink | 0777), Target: "../../target.txt"}))
	of := &objectFile{backend: backend, locks: newObjectLocks(), bucket: "bkt", key: "link", meta: obj}
	target, err := of.Readlink()
	require.NoError(t, err)
	require.Equal(t, "../../target.txt", target)
}

func TestObjectFile_Readlink_OnRegularFileReturnsEINVAL(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	obj, err := backend.PutObject(ctx, "bkt", "hello.txt", strings.NewReader("hello"), "text/plain")
	require.NoError(t, err)
	of := &objectFile{backend: backend, locks: newObjectLocks(), bucket: "bkt", key: "hello.txt", meta: obj}
	target, err := of.Readlink()
	require.ErrorIs(t, err, syscall.EINVAL)
	require.Equal(t, "", target)
}

func TestObjectFile_ReadAt_FullContent(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	obj, err := backend.PutObject(ctx, "bkt", "hello.txt", strings.NewReader("hello world"), "text/plain")
	require.NoError(t, err)

	of := &objectFile{backend: backend, bucket: "bkt", key: "hello.txt", meta: obj}
	buf := make([]byte, 11)
	n, err := of.ReadAt(buf, 0)
	require.NoError(t, err)
	require.Equal(t, 11, n)
	require.Equal(t, "hello world", string(buf))
}

func TestObjectFile_ReadAt_WithOffset(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	obj, err := backend.PutObject(ctx, "bkt", "hello.txt", strings.NewReader("hello world"), "text/plain")
	require.NoError(t, err)

	of := &objectFile{backend: backend, bucket: "bkt", key: "hello.txt", meta: obj}
	buf := make([]byte, 5)
	n, err := of.ReadAt(buf, 6)
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, "world", string(buf))
}

func TestObjectFile_Open_WriteMode(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	obj, err := backend.PutObject(ctx, "bkt", "hello.txt", strings.NewReader("hello"), "text/plain")
	require.NoError(t, err)

	of := &objectFile{backend: backend, locks: newObjectLocks(), bucket: "bkt", key: "hello.txt", meta: obj}
	_, _, err = of.Open(p9.WriteOnly)
	require.NoError(t, err)
}

func TestObjectFile_WriteAt_OverwritesAndExtends(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	obj, err := backend.PutObject(ctx, "bkt", "file.bin", strings.NewReader("abcdef"), "application/octet-stream")
	require.NoError(t, err)
	of := &objectFile{backend: backend, locks: newObjectLocks(), bucket: "bkt", key: "file.bin", meta: obj}
	n, err := of.WriteAt([]byte("XY"), 2)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	buf := make([]byte, 6)
	readN, err := of.ReadAt(buf, 0)
	require.NoError(t, err)
	require.Equal(t, 6, readN)
	require.Equal(t, []byte("abXYef"), buf)
	require.NoError(t, of.FSync())
	require.Equal(t, []byte("abXYef"), readObjectBytes(t, backend, "bkt", "file.bin"))
	n, err = of.WriteAt([]byte("Z"), 8)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.NoError(t, of.Close())
	require.Equal(t, []byte{'a', 'b', 'X', 'Y', 'e', 'f', 0, 0, 'Z'}, readObjectBytes(t, backend, "bkt", "file.bin"))
}

func TestObjectFile_WriteAt_ReadFailureDoesNotCreateSparseObject(t *testing.T) {
	backend := &getFailBackend{Backend: newTestBackend(t), err: syscall.EIO}
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	obj := &storage.Object{Key: "file.bin", Size: 3}
	of := &objectFile{backend: backend, locks: newObjectLocks(), bucket: "bkt", key: "file.bin", meta: obj}
	_, err := of.WriteAt([]byte("Z"), 1)
	require.ErrorIs(t, err, syscall.EIO)
}

func TestObjectFile_WriteAt_FallsBackWhenPartialIOIsNotPreferred(t *testing.T) {
	backend := &preferPutObjectBackend{Backend: newTestBackend(t)}
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	obj, err := backend.PutObject(ctx, "bkt", "file.txt", strings.NewReader("abc"), "text/plain")
	require.NoError(t, err)
	of := &objectFile{backend: backend, locks: newObjectLocks(), bucket: "bkt", key: "file.txt", meta: obj}

	n, err := of.WriteAt([]byte("Z"), 1)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.NoError(t, of.Close())
	require.Equal(t, []byte("aZc"), readObjectBytes(t, backend, "bkt", "file.txt"))
	require.False(t, backend.writeAtCalled)
}

func TestObjectFile_WriteAt_FallbackRejectsLargeExistingObject(t *testing.T) {
	backend := &largeHeadBackend{Backend: newTestBackend(t), size: maxFallbackObjectSize + 1}
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	obj, err := backend.Backend.PutObject(ctx, "bkt", "file.txt", strings.NewReader("abc"), "text/plain")
	require.NoError(t, err)
	of := &objectFile{backend: backend, locks: newObjectLocks(), bucket: "bkt", key: "file.txt", meta: obj}

	_, err = of.WriteAt([]byte("Z"), 1)
	require.ErrorIs(t, err, syscall.EFBIG)
	require.False(t, backend.getObjectCalled)
}

func TestObjectFile_ReadAt_UsesPartialIOWhenReadPreferred(t *testing.T) {
	backend := &preferReadAtBackend{Backend: newTestBackend(t)}
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	obj, err := backend.PutObject(ctx, "bkt", "file.txt", strings.NewReader("abcdef"), "text/plain")
	require.NoError(t, err)
	of := &objectFile{backend: backend, locks: newObjectLocks(), bucket: "bkt", key: "file.txt", meta: obj}

	buf := make([]byte, 3)
	n, err := of.ReadAt(buf, 2)
	require.NoError(t, err)
	require.Equal(t, 3, n)
	require.Equal(t, []byte("cde"), buf)
	require.True(t, backend.readAtCalled)
	require.False(t, backend.writeAtCalled)
}

func TestObjectFile_SetAttrSize_FallsBackWhenTruncateIsNotPreferred(t *testing.T) {
	backend := &preferPutObjectBackend{Backend: newTestBackend(t)}
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	obj, err := backend.PutObject(ctx, "bkt", "file.txt", strings.NewReader("abcdef"), "text/plain")
	require.NoError(t, err)
	of := &objectFile{backend: backend, locks: newObjectLocks(), bucket: "bkt", key: "file.txt", meta: obj}

	require.NoError(t, of.SetAttr(p9.SetAttrMask{Size: true}, p9.SetAttr{Size: 3}))
	require.Equal(t, []byte("abc"), readObjectBytes(t, backend, "bkt", "file.txt"))
	require.False(t, backend.truncateCalled)
}

func TestObjectFile_FSync_DelegatesToSyncable(t *testing.T) {
	backend := &syncCountingBackend{Backend: newTestBackend(t)}
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	obj, err := backend.PutObject(ctx, "bkt", "file.txt", strings.NewReader("data"), "text/plain")
	require.NoError(t, err)
	of := &objectFile{backend: backend, locks: newObjectLocks(), bucket: "bkt", key: "file.txt", meta: obj}
	require.NoError(t, of.FSync())
	require.Equal(t, 1, backend.syncCalls)
	require.Equal(t, "bkt", backend.syncBucket)
	require.Equal(t, "file.txt", backend.syncKey)
}

func TestObjectFile_FSync_ErrorReturnsEIO(t *testing.T) {
	backend := &syncCountingBackend{Backend: newTestBackend(t), err: syscall.EIO}
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	obj, err := backend.PutObject(ctx, "bkt", "file.txt", strings.NewReader("data"), "text/plain")
	require.NoError(t, err)
	of := &objectFile{backend: backend, locks: newObjectLocks(), bucket: "bkt", key: "file.txt", meta: obj}
	require.ErrorIs(t, of.FSync(), syscall.EIO)
}

// Task 5: Server / Attacher tests

func TestAttacher_AttachReturnsRootFile(t *testing.T) {
	backend := newTestBackend(t)
	att := &attacher{backend: backend}
	file, err := att.Attach()
	require.NoError(t, err)
	require.NotNil(t, file)
	_, ok := file.(*rootFile)
	require.True(t, ok)
}

func TestServer_ListenAndServe_Starts(t *testing.T) {
	backend := newTestBackend(t)
	srv := NewServer(backend)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := srv.ListenAndServe(ctx, "127.0.0.1:0")
	if err != nil {
		require.ErrorIs(t, err, context.DeadlineExceeded)
	}
}

func TestServer_CloseStopsListenAndServe(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	backend := newTestBackend(t)
	srv := NewServer(backend)
	done := make(chan error, 1)
	go func() {
		done <- srv.ListenAndServe(context.Background(), addr)
	}()

	require.Eventually(t, func() bool {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			return false
		}
		_ = conn.Close()
		return true
	}, time.Second, 10*time.Millisecond)

	require.NoError(t, srv.Close())
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("9p server did not stop after Close")
	}
}

func TestP9Locks_PropagateThroughWalk(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	_, err := backend.PutObject(ctx, "bkt", "file.txt", strings.NewReader("hi"), "text/plain")
	require.NoError(t, err)

	locks := newObjectLocks()
	root := &rootFile{backend: backend, locks: locks}
	_, file, err := root.Walk([]string{"bkt"})
	require.NoError(t, err)
	bf := file.(*bucketFile)
	require.Same(t, locks, bf.locks)

	_, file, err = bf.Walk([]string{"file.txt"})
	require.NoError(t, err)
	of := file.(*objectFile)
	require.Same(t, locks, of.locks)
}

func TestP9Locks_CleanupAfterUnlock(t *testing.T) {
	locks := newObjectLocks()
	unlock := locks.lock("bkt", "file.txt")
	require.Len(t, locks.locks, 1)
	unlock()
	require.Empty(t, locks.locks)
}

// Task 9: Benchmarks

// prepareBench9P creates a LocalBackend with one object of objSize bytes.
// Returns ready-to-use objectFile, bucketFile, rootFile pointing at "bench"/"bench.bin".
func prepareBench9P(b *testing.B, objSize int) (*objectFile, *bucketFile, *rootFile) {
	b.Helper()
	backend, err := storage.NewLocalBackend(b.TempDir())
	require.NoError(b, err)

	require.NoError(b, backend.CreateBucket(context.Background(), "bench"))
	data := make([]byte, objSize)
	obj, err := backend.PutObject(context.Background(), "bench", "bench.bin",
		bytes.NewReader(data), "application/octet-stream")
	require.NoError(b, err)

	root := &rootFile{backend: backend}
	bf := &bucketFile{backend: backend, bucket: "bench"}
	of := &objectFile{backend: backend, bucket: "bench", key: "bench.bin", meta: obj}
	return of, bf, root
}

// BenchmarkObjectFile_ReadAt measures PartialIO-backed random read throughput.
// LocalBackend implements storage.PartialIO so this exercises the fast path.
func BenchmarkObjectFile_ReadAt(b *testing.B) {
	cases := []struct {
		name string
		size int
	}{
		{"4KiB", 4 << 10},
		{"64KiB", 64 << 10},
		{"1MiB", 1 << 20},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			of, _, _ := prepareBench9P(b, tc.size)
			buf := make([]byte, tc.size)
			b.SetBytes(int64(tc.size))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				_, _ = of.ReadAt(buf, 0)
			}
		})
	}
}

func BenchmarkObjectFile_SequentialWriteAt(b *testing.B) {
	cases := []struct {
		name   string
		bucket string
	}{
		{"fast_internal_bucket", "__grainfs_vfs_default"},
		{"fallback_user_bucket", "bench"},
	}
	const (
		chunkSize = 4 << 10
		chunks    = 256
	)
	payload := bytes.Repeat([]byte("x"), chunkSize)
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.SetBytes(chunkSize * chunks)
			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				backend, err := storage.NewLocalBackend(b.TempDir())
				require.NoError(b, err)
				require.NoError(b, backend.CreateBucket(context.Background(), tc.bucket))
				obj, err := backend.PutObject(context.Background(), tc.bucket, "bench.bin",
					bytes.NewReader(nil), "application/octet-stream")
				require.NoError(b, err)
				of := &objectFile{backend: backend, locks: newObjectLocks(), bucket: tc.bucket, key: "bench.bin", meta: obj}
				b.StartTimer()
				for i := range chunks {
					_, err := of.WriteAt(payload, int64(i*chunkSize))
					require.NoError(b, err)
				}
				require.NoError(b, of.Close())
			}
		})
	}
}

// BenchmarkBucketFile_Readdir measures directory listing cost at various scales.
func BenchmarkBucketFile_Readdir(b *testing.B) {
	cases := []struct {
		name  string
		n     int
		count uint32
		key   func(int) string
	}{
		{"100objs", 100, 100, func(i int) string { return fmt.Sprintf("obj-%06d.bin", i) }},
		{"1000objs", 1000, 1000, func(i int) string { return fmt.Sprintf("obj-%06d.bin", i) }},
		{"1000objs_page100", 1000, 100, func(i int) string { return fmt.Sprintf("obj-%06d.bin", i) }},
		{"1000nested_one_dir", 1000, 100, func(i int) string { return fmt.Sprintf("obj/%06d.bin", i) }},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			backend, err := storage.NewLocalBackend(b.TempDir())
			require.NoError(b, err)
			require.NoError(b, backend.CreateBucket(context.Background(), "bench"))
			for i := range tc.n {
				_, err := backend.PutObject(context.Background(), "bench",
					tc.key(i), strings.NewReader("x"), "application/octet-stream")
				require.NoError(b, err)
			}
			bf := &bucketFile{backend: backend, bucket: "bench"}
			b.ResetTimer()
			b.ReportAllocs()
			for b.Loop() {
				_, _ = bf.Readdir(0, tc.count)
			}
		})
	}
}

// BenchmarkRootFile_Walk_Bucket measures bucket lookup latency (HeadBucket + QID alloc).
func BenchmarkRootFile_Walk_Bucket(b *testing.B) {
	_, _, root := prepareBench9P(b, 1)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, file, _ := root.Walk([]string{"bench"})
		if file != nil {
			_ = file.Close()
		}
	}
}

type countingWalkBackend struct {
	objects []*storage.Object
	walked  int
}

func (b *countingWalkBackend) CreateBucket(context.Context, string) error { return nil }
func (b *countingWalkBackend) HeadBucket(context.Context, string) error   { return nil }
func (b *countingWalkBackend) DeleteBucket(context.Context, string) error { return nil }
func (b *countingWalkBackend) ForceDeleteBucket(context.Context, string) error {
	return nil
}
func (b *countingWalkBackend) ListBuckets(context.Context) ([]string, error) { return nil, nil }
func (b *countingWalkBackend) PutObject(context.Context, string, string, io.Reader, string) (*storage.Object, error) {
	return nil, nil
}
func (b *countingWalkBackend) GetObject(context.Context, string, string) (io.ReadCloser, *storage.Object, error) {
	return nil, nil, nil
}
func (b *countingWalkBackend) HeadObject(context.Context, string, string) (*storage.Object, error) {
	return nil, nil
}
func (b *countingWalkBackend) DeleteObject(context.Context, string, string) error { return nil }
func (b *countingWalkBackend) ListObjects(context.Context, string, string, int) ([]*storage.Object, error) {
	return nil, nil
}
func (b *countingWalkBackend) WalkObjects(_ context.Context, _, _ string, fn func(*storage.Object) error) error {
	for _, obj := range b.objects {
		b.walked++
		if err := fn(obj); err != nil {
			return err
		}
	}
	return nil
}
func (b *countingWalkBackend) CreateMultipartUpload(context.Context, string, string, string) (*storage.MultipartUpload, error) {
	return nil, nil
}
func (b *countingWalkBackend) UploadPart(context.Context, string, string, string, int, io.Reader) (*storage.Part, error) {
	return nil, nil
}
func (b *countingWalkBackend) CompleteMultipartUpload(context.Context, string, string, string, []storage.Part) (*storage.Object, error) {
	return nil, nil
}
func (b *countingWalkBackend) AbortMultipartUpload(context.Context, string, string, string) error {
	return nil
}
func (b *countingWalkBackend) ListMultipartUploads(context.Context, string, string, int) ([]*storage.MultipartUpload, error) {
	return nil, nil
}
func (b *countingWalkBackend) ListParts(context.Context, string, string, string, int) ([]storage.Part, error) {
	return nil, nil
}

type keyWalkingBackend struct {
	countingWalkBackend
	keys          []string
	keysWalked    int
	objectsWalked int
}

func (b *keyWalkingBackend) WalkObjects(ctx context.Context, bucket, prefix string, fn func(*storage.Object) error) error {
	b.objectsWalked++
	return b.countingWalkBackend.WalkObjects(ctx, bucket, prefix, fn)
}

func (b *keyWalkingBackend) WalkObjectKeys(_ context.Context, _, _ string, fn func(string) error) error {
	for _, key := range b.keys {
		b.keysWalked++
		if err := fn(key); err != nil {
			return err
		}
	}
	return nil
}

type getFailBackend struct {
	storage.Backend
	err error
}

func (b *getFailBackend) GetObject(context.Context, string, string) (io.ReadCloser, *storage.Object, error) {
	return nil, nil, b.err
}

type largeHeadBackend struct {
	storage.Backend
	size            int64
	getObjectCalled bool
}

func (b *largeHeadBackend) HeadObject(ctx context.Context, bucket, key string) (*storage.Object, error) {
	obj, err := b.Backend.HeadObject(ctx, bucket, key)
	if err != nil {
		return nil, err
	}
	copied := *obj
	copied.Size = b.size
	return &copied, nil
}

func (b *largeHeadBackend) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, *storage.Object, error) {
	b.getObjectCalled = true
	return b.Backend.GetObject(ctx, bucket, key)
}

type preferPutObjectBackend struct {
	storage.Backend
	writeAtCalled   bool
	truncateCalled  bool
	preferWriteAtFn func(string) bool
}

func (b *preferPutObjectBackend) PreferWriteAt(bucket string) bool {
	if b.preferWriteAtFn != nil {
		return b.preferWriteAtFn(bucket)
	}
	return false
}

func (b *preferPutObjectBackend) WriteAt(context.Context, string, string, uint64, []byte) (*storage.Object, error) {
	b.writeAtCalled = true
	return nil, errors.New("partial write should not be used")
}

func (b *preferPutObjectBackend) ReadAt(context.Context, string, string, int64, []byte) (int, error) {
	return 0, errors.New("partial read should not be used")
}

func (b *preferPutObjectBackend) Truncate(context.Context, string, string, int64) error {
	b.truncateCalled = true
	return errors.New("truncate should not be used")
}

type preferReadAtBackend struct {
	storage.Backend
	readAtCalled  bool
	writeAtCalled bool
}

func (b *preferReadAtBackend) PreferReadAt(string) bool  { return true }
func (b *preferReadAtBackend) PreferWriteAt(string) bool { return false }

func (b *preferReadAtBackend) ReadAt(ctx context.Context, bucket, key string, offset int64, buf []byte) (int, error) {
	b.readAtCalled = true
	rc, _, err := b.Backend.GetObject(ctx, bucket, key)
	if err != nil {
		return 0, err
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return 0, err
	}
	return bytes.NewReader(data).ReadAt(buf, offset)
}

func (b *preferReadAtBackend) WriteAt(context.Context, string, string, uint64, []byte) (*storage.Object, error) {
	b.writeAtCalled = true
	return nil, errors.New("partial write should not be used")
}

func (b *preferReadAtBackend) Truncate(context.Context, string, string, int64) error {
	return errors.New("truncate should not be used")
}

type deleteMarkerHeadBackend struct {
	storage.Backend
	deleteMarkerKey string
}

func (b *deleteMarkerHeadBackend) HeadObject(ctx context.Context, bucket, key string) (*storage.Object, error) {
	obj, err := b.Backend.HeadObject(ctx, bucket, key)
	if err != nil {
		return nil, err
	}
	if key == b.deleteMarkerKey {
		copyObj := *obj
		copyObj.IsDeleteMarker = true
		return &copyObj, nil
	}
	return obj, nil
}

type syncCountingBackend struct {
	storage.Backend
	syncCalls  int
	syncBucket string
	syncKey    string
	err        error
}

func (b *syncCountingBackend) Sync(bucket, key string) error {
	b.syncCalls++
	b.syncBucket = bucket
	b.syncKey = key
	return b.err
}

func readObjectBytes(t testing.TB, backend storage.Backend, bucket, key string) []byte {
	t.Helper()
	rc, _, err := backend.GetObject(context.Background(), bucket, key)
	require.NoError(t, err)
	defer rc.Close()
	data, err := io.ReadAll(rc)
	require.NoError(t, err)
	return data
}
