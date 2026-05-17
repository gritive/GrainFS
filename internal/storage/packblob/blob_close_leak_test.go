package packblob

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBlobStore_Close_ReleasesDirLockOnError verifies that Close() releases
// the directory lock even when an intermediate fd close fails. Previously
// the active.Close() error path returned early, leaving readFiles open
// and the directory flock held — a follow-up NewBlobStore() in the same
// directory would block or fail.
func TestBlobStore_Close_ReleasesDirLockOnError(t *testing.T) {
	dir := t.TempDir()
	bs, err := NewBlobStore(dir, 1<<20)
	require.NoError(t, err)

	// Force the next Close() to error on the active blob: close the
	// underlying fd out from under it. The double-close will return
	// EBADF or "file already closed" — Close() must keep going.
	require.NoError(t, bs.active.Close())

	err = bs.Close()
	assert.Error(t, err, "Close() must surface the pre-closed active fd error")

	// The directory lock must be released so a fresh BlobStore can open.
	bs2, err := NewBlobStore(dir, 1<<20)
	require.NoError(t, err, "directory lock leaked — second NewBlobStore failed")
	require.NoError(t, bs2.Close())
}

// TestBlobStore_Close_AttemptsAllReadFiles verifies Close() closes every
// cached read fd even after one of them fails to close.
func TestBlobStore_Close_AttemptsAllReadFiles(t *testing.T) {
	dir := t.TempDir()
	bs, err := NewBlobStore(dir, 1<<20)
	require.NoError(t, err)

	// Seed two read-fd cache entries with real but pre-closed files so
	// each Close() call fails. Without the fix only the first one would
	// surface; the second would be silently skipped and the lock leaked.
	f1, err := os.CreateTemp(dir, "leak-test-1-*")
	require.NoError(t, err)
	require.NoError(t, f1.Close()) // force later Close() to fail

	f2, err := os.CreateTemp(dir, "leak-test-2-*")
	require.NoError(t, err)
	require.NoError(t, f2.Close()) // same

	cache := map[uint64]*os.File{42: f1, 43: f2}
	bs.readFiles.Store(&cache)

	err = bs.Close()
	require.Error(t, err)
	// Both fds must appear in the joined error — proves both were attempted.
	msg := err.Error()
	assert.Contains(t, msg, "blob 42")
	assert.Contains(t, msg, "blob 43")

	// And the dir lock must be released so a new BlobStore opens cleanly.
	bs2, err := NewBlobStore(dir, 1<<20)
	require.NoError(t, err)
	require.NoError(t, bs2.Close())
}
