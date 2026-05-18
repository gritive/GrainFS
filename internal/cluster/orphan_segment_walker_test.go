package cluster

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// backDate sets mtime to (now - dur) on path.
func backDate(t *testing.T, path string, dur time.Duration) {
	t.Helper()
	past := time.Now().Add(-dur)
	require.NoError(t, os.Chtimes(path, past, past))
}

func TestWalkOrphanSegments_Production(t *testing.T) {
	tmpRoot := t.TempDir()
	segDir := filepath.Join(tmpRoot, "data", "bucket", "key_segments")
	require.NoError(t, os.MkdirAll(segDir, 0o755))

	// 3 files: old (orphan), young (skip), known (skip).
	oldPath := filepath.Join(segDir, "blob-old")
	require.NoError(t, os.WriteFile(oldPath, []byte("seg"), 0o644))
	backDate(t, oldPath, 10*time.Minute)

	youngPath := filepath.Join(segDir, "blob-young")
	require.NoError(t, os.WriteFile(youngPath, []byte("seg"), 0o644))

	knownPath := filepath.Join(segDir, "blob-known")
	require.NoError(t, os.WriteFile(knownPath, []byte("seg"), 0o644))
	backDate(t, knownPath, 10*time.Minute)

	b := &DistributedBackend{root: tmpRoot, scrubOrphanAge: 5 * time.Minute}
	known := map[string]bool{"bucket/key_segments/blob-known": true}
	var found []string
	err := b.WalkOrphanSegments("bucket", known, func(p string) error {
		found = append(found, p)
		return nil
	})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"bucket/key_segments/blob-old"}, found)
}

func TestWalkOrphanSegments_BucketENOENT(t *testing.T) {
	tmpRoot := t.TempDir()
	b := &DistributedBackend{root: tmpRoot, scrubOrphanAge: 5 * time.Minute}
	err := b.WalkOrphanSegments("nonexistent", nil, func(string) error { return nil })
	require.NoError(t, err)
}

func TestDeleteOrphanSegment_Production(t *testing.T) {
	tmpRoot := t.TempDir()
	segPath := filepath.Join(tmpRoot, "data", "bucket", "key_segments", "blob1")
	require.NoError(t, os.MkdirAll(filepath.Dir(segPath), 0o755))
	require.NoError(t, os.WriteFile(segPath, []byte("seg"), 0o644))

	b := &DistributedBackend{root: tmpRoot, scrubOrphanAge: 5 * time.Minute}

	require.NoError(t, b.DeleteOrphanSegment("bucket/key_segments/blob1"))
	_, statErr := os.Stat(segPath)
	require.True(t, os.IsNotExist(statErr))

	// Second delete: ENOENT swallow.
	require.NoError(t, b.DeleteOrphanSegment("bucket/key_segments/blob1"))
}

func TestWalkOrphanSegments_ErrorPaths(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("permission test requires non-root")
	}
	tmpRoot := t.TempDir()
	segDir := filepath.Join(tmpRoot, "data", "bucket", "blocked_segments")
	require.NoError(t, os.MkdirAll(segDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(segDir, "blob1"), []byte("s"), 0o644))

	require.NoError(t, os.Chmod(segDir, 0o000))
	defer os.Chmod(segDir, 0o755)

	b := &DistributedBackend{root: tmpRoot, scrubOrphanAge: 5 * time.Minute}
	var found []string
	err := b.WalkOrphanSegments("bucket", nil, func(p string) error {
		found = append(found, p)
		return nil
	})
	require.NoError(t, err) // walker swallows readdir error per segDir
	require.Empty(t, found)
}
