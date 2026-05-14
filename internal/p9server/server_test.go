package p9server

import (
	"context"
	"strings"
	"syscall"
	"testing"

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

func TestObjectFile_Open_WriteMode_EROFS(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "bkt"))
	obj, err := backend.PutObject(ctx, "bkt", "hello.txt", strings.NewReader("hello"), "text/plain")
	require.NoError(t, err)

	of := &objectFile{backend: backend, bucket: "bkt", key: "hello.txt", meta: obj}
	_, _, err = of.Open(p9.WriteOnly)
	require.ErrorIs(t, err, syscall.EROFS)
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
