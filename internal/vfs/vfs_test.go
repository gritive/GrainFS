package vfs

import (
	"io"
	"os"
	"sort"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupFS(t *testing.T) *GrainVFS {
	t.Helper()
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)
	fs, err := New(backend, "test-vol")
	require.NoError(t, err)
	return fs
}

func TestCreateAndReadFile(t *testing.T) {
	fs := setupFS(t)

	f, err := fs.Create("hello.txt")
	require.NoError(t, err)

	_, err = f.Write([]byte("Hello, GrainFS!"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Read it back
	f2, err := fs.Open("hello.txt")
	require.NoError(t, err)

	data, err := io.ReadAll(f2)
	require.NoError(t, err)
	assert.Equal(t, "Hello, GrainFS!", string(data))
	require.NoError(t, f2.Close())
}

func TestOpenFile(t *testing.T) {
	fs := setupFS(t)

	// Create and write
	f, err := fs.OpenFile("test.txt", os.O_CREATE|os.O_WRONLY, 0644)
	require.NoError(t, err)
	_, err = f.Write([]byte("data"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Open for reading
	f2, err := fs.OpenFile("test.txt", os.O_RDONLY, 0)
	require.NoError(t, err)
	data, err := io.ReadAll(f2)
	require.NoError(t, err)
	assert.Equal(t, "data", string(data))
	require.NoError(t, f2.Close())
}

func TestStat(t *testing.T) {
	fs := setupFS(t)

	f, err := fs.Create("stat-test.txt")
	require.NoError(t, err)
	_, err = f.Write([]byte("12345"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	info, err := fs.Stat("stat-test.txt")
	require.NoError(t, err)
	assert.Equal(t, "stat-test.txt", info.Name())
	assert.Equal(t, int64(5), info.Size())
	assert.False(t, info.IsDir())
}

func TestStatNonexistent(t *testing.T) {
	fs := setupFS(t)
	_, err := fs.Stat("nonexistent.txt")
	assert.Error(t, err)
}

func TestRemove(t *testing.T) {
	fs := setupFS(t)

	f, err := fs.Create("remove-me.txt")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	err = fs.Remove("remove-me.txt")
	require.NoError(t, err)

	_, err = fs.Stat("remove-me.txt")
	assert.Error(t, err)
}

func TestRename(t *testing.T) {
	fs := setupFS(t)

	f, err := fs.Create("old-name.txt")
	require.NoError(t, err)
	_, err = f.Write([]byte("content"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	err = fs.Rename("old-name.txt", "new-name.txt")
	require.NoError(t, err)

	_, err = fs.Stat("old-name.txt")
	assert.Error(t, err)

	f2, err := fs.Open("new-name.txt")
	require.NoError(t, err)
	data, err := io.ReadAll(f2)
	require.NoError(t, err)
	assert.Equal(t, "content", string(data))
	require.NoError(t, f2.Close())
}

func TestMkdirAllAndReadDir(t *testing.T) {
	fs := setupFS(t)

	err := fs.MkdirAll("a/b/c", 0755)
	require.NoError(t, err)

	// Create files in directories
	f, err := fs.Create("a/file1.txt")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	f, err = fs.Create("a/file2.txt")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	entries, err := fs.ReadDir("a")
	require.NoError(t, err)

	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)

	// Should contain at least the files and the subdirectory
	assert.Contains(t, names, "b")
	assert.Contains(t, names, "file1.txt")
	assert.Contains(t, names, "file2.txt")
}

func TestReadDirRoot(t *testing.T) {
	fs := setupFS(t)

	f, err := fs.Create("root-file.txt")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	entries, err := fs.ReadDir("/")
	require.NoError(t, err)
	assert.NotEmpty(t, entries)
}

func TestStatDirectory(t *testing.T) {
	fs := setupFS(t)

	err := fs.MkdirAll("mydir", 0755)
	require.NoError(t, err)

	info, err := fs.Stat("mydir")
	require.NoError(t, err)
	assert.True(t, info.IsDir())
}

func TestChroot(t *testing.T) {
	fs := setupFS(t)

	err := fs.MkdirAll("sub", 0755)
	require.NoError(t, err)

	f, err := fs.Create("sub/inner.txt")
	require.NoError(t, err)
	_, err = f.Write([]byte("inner data"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	sub, err := fs.Chroot("sub")
	require.NoError(t, err)

	f2, err := sub.Open("inner.txt")
	require.NoError(t, err)
	data, err := io.ReadAll(f2)
	require.NoError(t, err)
	assert.Equal(t, "inner data", string(data))
	require.NoError(t, f2.Close())
}

func TestRoot(t *testing.T) {
	fs := setupFS(t)
	assert.Equal(t, "/", fs.Root())
}

func TestSeek(t *testing.T) {
	fs := setupFS(t)

	f, err := fs.Create("seek-test.txt")
	require.NoError(t, err)
	_, err = f.Write([]byte("abcdefghij"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	f2, err := fs.Open("seek-test.txt")
	require.NoError(t, err)

	// Seek to position 5
	pos, err := f2.Seek(5, io.SeekStart)
	require.NoError(t, err)
	assert.Equal(t, int64(5), pos)

	buf := make([]byte, 5)
	n, err := f2.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, "fghij", string(buf))
	require.NoError(t, f2.Close())
}

func TestTruncate(t *testing.T) {
	fs := setupFS(t)

	f, err := fs.Create("trunc.txt")
	require.NoError(t, err)
	_, err = f.Write([]byte("1234567890"))
	require.NoError(t, err)

	err = f.Truncate(5)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	info, err := fs.Stat("trunc.txt")
	require.NoError(t, err)
	assert.Equal(t, int64(5), info.Size())
}
