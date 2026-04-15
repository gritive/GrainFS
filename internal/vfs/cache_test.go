package vfs

import (
	"io"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupCachedFS(t *testing.T, ttl time.Duration) *GrainVFS {
	t.Helper()
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)
	fs, err := New(backend, "cache-test", WithStatCacheTTL(ttl), WithDirCacheTTL(ttl))
	require.NoError(t, err)
	return fs
}

func TestStatCacheHit(t *testing.T) {
	fs := setupCachedFS(t, 5*time.Second)

	f, err := fs.Create("cached.txt")
	require.NoError(t, err)
	_, _ = f.Write([]byte("hello"))
	require.NoError(t, f.Close())

	// First Stat: populates cache
	info1, err := fs.Stat("cached.txt")
	require.NoError(t, err)
	assert.Equal(t, int64(5), info1.Size())

	// Second Stat: should hit cache (same result)
	info2, err := fs.Stat("cached.txt")
	require.NoError(t, err)
	assert.Equal(t, info1.Size(), info2.Size())
	assert.Equal(t, info1.Name(), info2.Name())
}

func TestStatCacheInvalidatedOnWrite(t *testing.T) {
	fs := setupCachedFS(t, 5*time.Second)

	f, err := fs.Create("mut.txt")
	require.NoError(t, err)
	_, _ = f.Write([]byte("v1"))
	require.NoError(t, f.Close())

	// Populate stat cache
	info1, err := fs.Stat("mut.txt")
	require.NoError(t, err)
	assert.Equal(t, int64(2), info1.Size())

	// Overwrite with larger content
	f2, err := fs.Create("mut.txt")
	require.NoError(t, err)
	_, _ = f2.Write([]byte("version2"))
	require.NoError(t, f2.Close())

	// Stat should reflect new size
	info2, err := fs.Stat("mut.txt")
	require.NoError(t, err)
	assert.Equal(t, int64(8), info2.Size())
}

func TestStatCacheInvalidatedOnRemove(t *testing.T) {
	fs := setupCachedFS(t, 5*time.Second)

	f, err := fs.Create("gone.txt")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Populate cache
	_, err = fs.Stat("gone.txt")
	require.NoError(t, err)

	require.NoError(t, fs.Remove("gone.txt"))

	// Stat should fail
	_, err = fs.Stat("gone.txt")
	assert.Error(t, err)
}

func TestReadDirCacheHit(t *testing.T) {
	fs := setupCachedFS(t, 5*time.Second)

	f, err := fs.Create("file1.txt")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	f, err = fs.Create("file2.txt")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// First ReadDir: populates cache
	entries1, err := fs.ReadDir("/")
	require.NoError(t, err)
	assert.Len(t, entries1, 2)

	// Second ReadDir: cache hit
	entries2, err := fs.ReadDir("/")
	require.NoError(t, err)
	assert.Len(t, entries2, 2)
}

func TestDirCacheInvalidatedOnCreate(t *testing.T) {
	fs := setupCachedFS(t, 5*time.Second)

	f, err := fs.Create("first.txt")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Populate dir cache
	entries, err := fs.ReadDir("/")
	require.NoError(t, err)
	assert.Len(t, entries, 1)

	// Create another file
	f2, err := fs.Create("second.txt")
	require.NoError(t, err)
	require.NoError(t, f2.Close())

	// ReadDir should see the new file
	entries2, err := fs.ReadDir("/")
	require.NoError(t, err)
	assert.Len(t, entries2, 2)
}

func TestIsDirCacheHit(t *testing.T) {
	fs := setupCachedFS(t, 5*time.Second)

	require.NoError(t, fs.MkdirAll("mydir", 0755))

	// First Stat on dir: populates isDir cache
	info, err := fs.Stat("mydir")
	require.NoError(t, err)
	assert.True(t, info.IsDir())

	// Second Stat: should hit cache
	info2, err := fs.Stat("mydir")
	require.NoError(t, err)
	assert.True(t, info2.IsDir())
}

func TestStatCacheExpiration(t *testing.T) {
	fs := setupCachedFS(t, 50*time.Millisecond)

	f, err := fs.Create("ttl.txt")
	require.NoError(t, err)
	_, _ = f.Write([]byte("data"))
	require.NoError(t, f.Close())

	_, err = fs.Stat("ttl.txt")
	require.NoError(t, err)

	// Wait for TTL to expire
	time.Sleep(100 * time.Millisecond)

	// Stat still works (just fetches fresh from backend)
	info, err := fs.Stat("ttl.txt")
	require.NoError(t, err)
	assert.Equal(t, int64(4), info.Size())
}

func TestNoCacheByDefault(t *testing.T) {
	// Default VFS (no options) should still work correctly
	fs := setupFS(t)

	f, err := fs.Create("no-cache.txt")
	require.NoError(t, err)
	_, _ = f.Write([]byte("abc"))
	require.NoError(t, f.Close())

	info, err := fs.Stat("no-cache.txt")
	require.NoError(t, err)
	assert.Equal(t, int64(3), info.Size())

	entries, err := fs.ReadDir("/")
	require.NoError(t, err)
	assert.NotEmpty(t, entries)
}

func TestRenamInvalidatesCache(t *testing.T) {
	fs := setupCachedFS(t, 5*time.Second)

	f, err := fs.Create("old.txt")
	require.NoError(t, err)
	_, _ = f.Write([]byte("data"))
	require.NoError(t, f.Close())

	// Populate cache
	_, err = fs.Stat("old.txt")
	require.NoError(t, err)

	require.NoError(t, fs.Rename("old.txt", "new.txt"))

	_, err = fs.Stat("old.txt")
	assert.Error(t, err)

	info, err := fs.Stat("new.txt")
	require.NoError(t, err)
	assert.Equal(t, int64(4), info.Size())
}

func TestCachedVFSOpenRead(t *testing.T) {
	fs := setupCachedFS(t, 5*time.Second)

	f, err := fs.Create("read.txt")
	require.NoError(t, err)
	_, _ = f.Write([]byte("cached read"))
	require.NoError(t, f.Close())

	f2, err := fs.Open("read.txt")
	require.NoError(t, err)
	data, _ := io.ReadAll(f2)
	assert.Equal(t, "cached read", string(data))
	require.NoError(t, f2.Close())
}
